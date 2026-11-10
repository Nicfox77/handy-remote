//! Parakeet live-audio protocol. A completed `finish` barrier, rather than a
//! partial preview, is the authority for the result of a whole recording.
use super::{LiveOutcome, StreamCmd, StreamFinalizeReply};
use crate::engine_supervisor::EngineError;
use crate::settings::AppSettings;
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::mpsc;
use std::time::Duration;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

const IO_TIMEOUT: Duration = Duration::from_secs(3);
const FINALIZE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) fn enabled(settings: &AppSettings) -> bool {
    settings.remote_transcription_enabled
        && !settings
            .remote_transcription_streaming_url
            .trim()
            .is_empty()
        && !settings.translate_to_english
        && matches!(settings.selected_language.as_str(), "auto" | "en")
}

/// Retain finals for every utterance, including repeated spoken phrases.
/// An ID deduplicates retransmissions, never text equality.
#[derive(Default)]
struct Transcript {
    finals: Vec<(String, String)>,
    partial: String,
}

impl Transcript {
    fn event(&mut self, event: &Value) -> Result<bool> {
        match event["type"].as_str() {
            Some("partial") => {
                self.partial = event["text"]
                    .as_str()
                    .context("Invalid partial text")?
                    .into();
            }
            Some("final") => {
                let id = event["utteranceId"]
                    .as_str()
                    .context("Missing utterance ID")?;
                let text = event["text"].as_str().context("Invalid final text")?.trim();
                if let Some((_, previous)) = self.finals.iter_mut().find(|(key, _)| key == id) {
                    *previous = text.into();
                } else {
                    self.finals.push((id.into(), text.into()));
                }
                self.partial.clear();
            }
            Some("finished") => return Ok(true),
            Some("error") => bail!("Remote streaming processor reported an error"),
            _ => {}
        }
        Ok(false)
    }

    fn text(&self) -> String {
        self.finals
            .iter()
            .map(|(_, text)| text.as_str())
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

pub(super) async fn run(
    settings: &AppSettings,
    rx: &mpsc::Receiver<StreamCmd>,
    cancelled: impl Fn() -> bool,
    preview: impl Fn(&str, &str),
    ready: impl Fn(),
) {
    let mut reply = None;
    let result = tokio::select! {
        result = transport(settings, rx, &mut reply, preview, ready) => result,
        _ = async { while !cancelled() { tokio::time::sleep(Duration::from_millis(10)).await; } } => {
            Err(anyhow::Error::new(EngineError::Cancelled))
        }
    };
    let result = if cancelled() {
        Err(EngineError::Cancelled)
    } else {
        Ok(Some(LiveOutcome::Remote(result.map_err(|_| {
            // Do not include URLs, headers or remote response bodies in errors.
            "Remote stream failed or did not confirm the complete recording".to_string()
        }))))
    };
    if let Some(reply) = reply {
        let _ = reply.send(result);
        return;
    }
    // A failed connection must still consume the stop handshake. The recorder
    // keeps the full PCM independently; dropping these frames loses no audio.
    while let Ok(command) = rx.recv() {
        match command {
            StreamCmd::Feed(_) => {}
            StreamCmd::Finalize { reply, .. } => {
                let _ = reply.send(result);
                return;
            }
            StreamCmd::Cancel => return,
        }
    }
}

async fn transport(
    settings: &AppSettings,
    rx: &mpsc::Receiver<StreamCmd>,
    reply: &mut Option<mpsc::Sender<StreamFinalizeReply>>,
    preview: impl Fn(&str, &str),
    ready: impl Fn(),
) -> Result<String> {
    let url = settings.remote_transcription_streaming_url.trim();
    let parsed = reqwest::Url::parse(url)?;
    if !matches!(parsed.scheme(), "ws" | "wss")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        bail!("Invalid streaming URL");
    }
    let mut request = url.into_client_request()?;
    if !settings.remote_transcription_api_key.trim().is_empty() {
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", settings.remote_transcription_api_key.trim()).parse()?,
        );
    }
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(65536))
        .max_frame_size(Some(65536));
    let (mut socket, _) = timeout(
        IO_TIMEOUT,
        tokio_tungstenite::connect_async_with_config(request, Some(config), false),
    )
    .await??;
    let message = timeout(IO_TIMEOUT, socket.next())
        .await?
        .context("No ready event")??;
    let Message::Text(text) = message else {
        bail!("Expected ready event");
    };
    let event: Value = serde_json::from_str(&text)?;
    if event["type"] != "ready" || event["sampleRate"] != 16000 || event["dictationFinish"] != true
    {
        bail!("Server does not support confirmed dictation finalization");
    }
    ready();
    let mut transcript = Transcript::default();
    let mut samples = 0usize;
    let mut finalized = None;
    let mut ping_at = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    loop {
        tokio::select! {
            message = socket.next() => {
                match message.context("Stream disconnected")?? {
                    Message::Text(text) => {
                        let event: Value = serde_json::from_str(&text)?;
                        let finished = transcript.event(&event)?;
                        if finished {
                            if finalized.is_none() { bail!("Unsolicited finish event"); }
                            let _ = timeout(IO_TIMEOUT, socket.close(None)).await;
                            return Ok(transcript.text());
                        }
                        preview(&transcript.text(), &transcript.partial);
                    }
                    Message::Close(_) => bail!("Stream closed before finish acknowledgement"),
                    Message::Binary(_) => bail!("Unexpected playback audio"),
                    _ => {}
                }
            }
            _ = tick.tick() => {
                if finalized.is_some_and(|at: Instant| at.elapsed() >= FINALIZE_TIMEOUT) {
                    bail!("Stream finalization timed out");
                }
                if finalized.is_none() {
                    // Limit work per tick so incoming events and cancellation
                    // remain responsive while catching up after a short stall.
                    for _ in 0..32 {
                        match rx.try_recv() {
                            Ok(StreamCmd::Feed(pcm)) => {
                                samples += pcm.len();
                                if samples > 16000 * 600 { bail!("Recording exceeds ten minutes"); }
                                let bytes: Vec<u8> = pcm.iter().flat_map(|s|
                                    ((s.clamp(-1.0, 1.0) * 32767.0) as i16).to_le_bytes()).collect();
                                for chunk in bytes.chunks(6400) {
                                    timeout(IO_TIMEOUT, socket.send(Message::Binary(chunk.to_vec().into()))).await??;
                                }
                            }
                            Ok(StreamCmd::Finalize { reply: sender, samples: expected }) => {
                                *reply = Some(sender);
                                if samples != expected { bail!("Incomplete streamed audio"); }
                                timeout(IO_TIMEOUT, socket.send(Message::Text(json!({"type":"finish"}).to_string().into()))).await??;
                                finalized = Some(Instant::now());
                                break;
                            }
                            Ok(StreamCmd::Cancel) => return Err(EngineError::Cancelled.into()),
                            Err(mpsc::TryRecvError::Disconnected) => bail!("Recording closed"),
                            Err(mpsc::TryRecvError::Empty) => break,
                        }
                    }
                }
                if ping_at.elapsed() > Duration::from_secs(10) {
                    timeout(IO_TIMEOUT, socket.send(Message::Text(json!({"type":"ping"}).to_string().into()))).await??;
                    ping_at = Instant::now();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    #[allow(clippy::result_large_err)] // Tungstenite fixes its callback error type.
    fn transport_confirms_all_frames_and_never_accepts_partial_only_results() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            for mode in ["success", "empty", "disconnect", "legacy", "cancel", "missing_audio"] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("ws://{}/v1/audio/stream", listener.local_addr().unwrap());
                let cancelled = Arc::new(AtomicBool::new(false));
                let server_cancelled = cancelled.clone();
                let server = tokio::spawn(async move {
                    let (tcp, _) = listener.accept().await.unwrap();
                    let mut socket = tokio_tungstenite::accept_hdr_async(tcp,
                        |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                            assert_eq!(request.headers()["Authorization"], "Bearer synthetic-fixture-key");
                            Ok(response)
                        }).await.unwrap();
                    socket.send(Message::Text(json!({"type":"ready", "sampleRate":16000,
                        "dictationFinish":mode != "legacy"}).to_string().into())).await.unwrap();
                    if mode == "legacy" || mode == "missing_audio" {
                        // The client must refuse unconfirmed or incomplete results.
                        while socket.next().await.is_some_and(|message| message.is_ok_and(|m| !m.is_close())) {}
                        return;
                    }
                    socket.send(Message::Text(json!({"type":"partial", "utteranceId":"a", "text":"preview only"}).to_string().into())).await.unwrap();
                    if mode == "disconnect" { socket.close(None).await.unwrap(); return; }
                    let mut bytes = 0;
                    while let Some(Ok(message)) = socket.next().await {
                        match message {
                            Message::Binary(audio) => bytes += audio.len(),
                            Message::Text(text) if serde_json::from_str::<Value>(&text).unwrap()["type"] == "finish" => {
                                assert_eq!(bytes, 6400);
                                if mode == "cancel" {
                                    server_cancelled.store(true, Ordering::Release);
                                    // Keep the connection open until client cancellation.
                                    while socket.next().await.is_some_and(|message| message.is_ok_and(|m| !m.is_close())) {}
                                    return;
                                }
                                if mode == "success" {
                                    socket.send(Message::Text(json!({"type":"final", "utteranceId":"a", "text":"confirmed result"}).to_string().into())).await.unwrap();
                                }
                                socket.send(Message::Text(json!({"type":"finished"}).to_string().into())).await.unwrap();
                            }
                            Message::Close(_) => return,
                            _ => {}
                        }
                    }
                });
                let settings = AppSettings {
                    remote_transcription_streaming_url: url,
                    remote_transcription_api_key: crate::settings::SecretString::new("synthetic-fixture-key"),
                    ..Default::default()
                };
                let (tx, rx) = mpsc::channel();
                let (reply, result) = mpsc::channel();
                tx.send(StreamCmd::Feed(vec![0.1; 3200])).unwrap();
                tx.send(StreamCmd::Finalize { reply, samples: if mode == "missing_audio" { 3201 } else { 3200 } }).unwrap();
                timeout(Duration::from_secs(5), run(&settings, &rx,
                    || cancelled.load(Ordering::Acquire), |_, _| {}, || {})).await.unwrap();
                let result = result.recv_timeout(Duration::from_secs(1)).unwrap();
                match mode {
                    "success" => assert!(matches!(result, Ok(Some(LiveOutcome::Remote(Ok(text)))) if text == "confirmed result")),
                    "empty" => assert!(matches!(result, Ok(Some(LiveOutcome::Remote(Ok(text)))) if text.is_empty())),
                    "cancel" => assert!(matches!(result, Err(EngineError::Cancelled))),
                    _ => assert!(matches!(result, Ok(Some(LiveOutcome::Remote(Err(_)))))),
                }
                timeout(Duration::from_secs(2), server).await.unwrap().unwrap();
            }
        });
    }

    #[test]
    #[ignore = "Requires an explicitly configured server, private key file and public audio fixture"]
    fn real_parakeet_stream() {
        let key_path = std::env::var("HANDY_TEST_REMOTE_KEY_FILE").unwrap();
        let fixture = std::env::var("HANDY_TEST_REMOTE_AUDIO").unwrap();
        let url = std::env::var("HANDY_TEST_REMOTE_STREAM_URL").unwrap();
        let settings = AppSettings {
            remote_transcription_streaming_url: url,
            remote_transcription_api_key: crate::settings::SecretString::new(
                std::fs::read_to_string(key_path).unwrap().trim(),
            ),
            ..Default::default()
        };
        let mut reader = hound::WavReader::open(fixture).unwrap();
        assert_eq!(reader.spec().sample_rate, 16000);
        assert_eq!(reader.spec().channels, 1);
        let samples: Vec<f32> = reader
            .samples::<i16>()
            .map(|s| s.unwrap() as f32 / 32768.0)
            .collect();
        let (tx, rx) = mpsc::channel();
        let (reply, result) = mpsc::channel();
        let started = std::time::Instant::now();
        let sent = std::thread::spawn(move || {
            let count = samples.len();
            for chunk in samples.chunks(1280) {
                tx.send(StreamCmd::Feed(chunk.to_vec())).unwrap();
                std::thread::sleep(Duration::from_millis(80));
            }
            tx.send(StreamCmd::Finalize {
                reply,
                samples: count,
            })
            .unwrap();
        });
        let previews = std::sync::atomic::AtomicUsize::new(0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run(
            &settings,
            &rx,
            || false,
            |_, tentative| {
                if !tentative.is_empty() {
                    previews.fetch_add(1, Ordering::Relaxed);
                }
            },
            || {},
        ));
        sent.join().unwrap();
        let text = match result.recv().unwrap().unwrap().unwrap() {
            LiveOutcome::Remote(Ok(text)) => text,
            _ => panic!("Real remote stream failed"),
        };
        assert!(text.to_lowercase().contains("quick brown fox"));
        assert!(previews.load(Ordering::Relaxed) > 0);
        println!(
            "Public fixture: {} characters, {} previews, {:.2}s total",
            text.len(),
            previews.load(Ordering::Relaxed),
            started.elapsed().as_secs_f64()
        );
    }

    #[test]
    fn finals_keep_repeated_phrases_but_deduplicate_utterance_ids() {
        let mut transcript = Transcript::default();
        for id in ["a", "a", "b"] {
            transcript
                .event(&json!({"type":"final", "utteranceId":id, "text":"Try again."}))
                .unwrap();
        }
        transcript
            .event(&json!({"type":"partial", "text":"unfinished"}))
            .unwrap();
        assert_eq!(transcript.text(), "Try again. Try again.");
        assert!(transcript.event(&json!({"type":"finished"})).unwrap());
        assert_eq!(transcript.text(), "Try again. Try again.");
    }

    #[test]
    fn malformed_results_and_errors_cannot_be_successful_empty_results() {
        let mut transcript = Transcript::default();
        assert!(transcript
            .event(&json!({"type":"final", "text":"missing id"}))
            .is_err());
        assert!(transcript
            .event(&json!({"type":"error", "message":"private text"}))
            .is_err());
    }
}
