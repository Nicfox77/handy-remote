//! Bounded recovery for Canary 180M Flash's premature empty decodes.
//! Original PCM is retained for the independent model fallback.

use crate::engine_supervisor::EngineError;
use log::warn;
use transcribe_cpp::Transcript;

pub(super) const PARAKEET_MODEL: &str =
    "handy-computer/parakeet-unified-en-0.6b-gguf/parakeet-unified-en-0.6b-Q8_0.gguf";
const SAMPLE_RATE: usize = 16_000;

/// Detect a sustained, nearly constant rail at microphone startup, rather
/// than ordinary clipped speech. High-pass filtering removes its DC offset
/// without cutting samples or throwing away the beginning of a spoken word.
pub(super) fn clean_startup_dc(pcm: &[f32]) -> Vec<f32> {
    let frame_len = SAMPLE_RATE / 50;
    let frames = &pcm[..pcm.len().min(SAMPLE_RATE / 4)];
    let mut consecutive = 0;
    let mut rail = false;
    for frame in frames.chunks_exact(frame_len) {
        let mean = frame.iter().sum::<f32>() / frame_len as f32;
        let variance = frame.iter().map(|x| (x - mean).powi(2)).sum::<f32>() / frame_len as f32;
        if mean.abs() > 0.9 && variance < 0.0001 {
            consecutive += 1;
            if consecutive >= 3 {
                rail = true;
                break;
            }
        } else {
            consecutive = 0;
        }
    }
    if !rail {
        return pcm.to_vec();
    }

    warn!("Canary recovery: removing a sustained microphone-startup DC transient");
    // First-order 20 Hz high-pass, below the speech band. Filter the whole
    // clip so there is no splice at the end of the transient.
    let rc = 1.0 / (2.0 * std::f32::consts::PI * 20.0);
    let alpha = rc / (rc + 1.0 / SAMPLE_RATE as f32);
    let mut previous_input = pcm.first().copied().unwrap_or(0.0);
    let mut previous_output = 0.0;
    pcm.iter()
        .map(|&input| {
            let output = alpha * (previous_output + input - previous_input);
            previous_input = input;
            previous_output = output;
            output.clamp(-1.0, 1.0)
        })
        .collect()
}

/// One padded retry, then one independent-model fallback. Cancellation is
/// terminal at either step. Even a successful engine call must contain text;
/// speech that exhausted recovery is an error, never a silent success.
pub(super) fn recover_empty(
    original: Vec<f32>,
    cleaned: Vec<f32>,
    retry: impl FnOnce(Vec<f32>) -> Result<Transcript, EngineError>,
    fallback: impl FnOnce(Vec<f32>) -> Result<Transcript, EngineError>,
) -> Result<Transcript, EngineError> {
    let mut padded = vec![0.0; SAMPLE_RATE];
    padded.extend(cleaned);
    warn!("Canary returned no text for speech; retrying once with one second of leading silence");
    match retry(padded) {
        Ok(transcript) if !transcript.text.trim().is_empty() => return Ok(transcript),
        Err(EngineError::Cancelled) => return Err(EngineError::Cancelled),
        Err(error) => warn!("Canary padded retry failed: {error}"),
        Ok(_) => warn!("Canary padded retry was empty; using Parakeet on the original audio"),
    }
    let transcript = fallback(original)?;
    if transcript.text.trim().is_empty() {
        return Err(EngineError::Failed(
            "Speech was detected, but Canary and the Parakeet fallback both returned no text. Retry with another model using the original recording.".into(),
        ));
    }
    Ok(transcript)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> Transcript {
        Transcript {
            text: value.into(),
            ..Default::default()
        }
    }

    #[test]
    fn padded_retry_retains_every_input_sample_and_avoids_fallback_when_successful() {
        let original = vec![0.1, -0.3, 0.2];
        let expected = original.clone();
        let result = recover_empty(
            original.clone(),
            original,
            |padded| {
                assert_eq!(&padded[..SAMPLE_RATE], vec![0.0; SAMPLE_RATE]);
                assert_eq!(&padded[SAMPLE_RATE..], expected);
                Ok(text("Recovered speech"))
            },
            |_| panic!("successful retry must not load Parakeet"),
        )
        .unwrap();
        assert_eq!(result.text, "Recovered speech");
    }

    #[test]
    fn fallback_receives_original_audio_after_empty_retry() {
        let original = vec![0.1, -0.3, 0.2];
        let expected = original.clone();
        let result = recover_empty(
            original,
            vec![0.0; 3],
            |_| Ok(text("  ")),
            |pcm| {
                assert_eq!(pcm, expected);
                Ok(text("Parakeet recovered speech"))
            },
        )
        .unwrap();
        assert_eq!(result.text, "Parakeet recovered speech");
    }

    #[test]
    fn cancelled_retry_never_starts_fallback() {
        assert!(matches!(
            recover_empty(
                vec![0.1],
                vec![0.1],
                |_| Err(EngineError::Cancelled),
                |_| panic!("cancel must not start another model")
            ),
            Err(EngineError::Cancelled)
        ));
    }

    #[test]
    fn retry_failure_can_recover_but_empty_fallback_is_an_error() {
        let result = recover_empty(
            vec![0.1],
            vec![0.1],
            |_| Err(EngineError::Failed("worker failed".into())),
            |_| Ok(text("fallback speech")),
        )
        .unwrap();
        assert_eq!(result.text, "fallback speech");
        assert!(matches!(
            recover_empty(vec![0.1], vec![0.1], |_| Ok(text("")), |_| Ok(text(""))),
            Err(EngineError::Failed(_))
        ));
    }

    #[test]
    fn fallback_errors_and_cancellation_are_not_returned_as_empty_successes() {
        assert!(matches!(
            recover_empty(
                vec![0.1],
                vec![0.1],
                |_| Ok(text("")),
                |_| Err(EngineError::Cancelled)
            ),
            Err(EngineError::Cancelled)
        ));
        assert!(matches!(
            recover_empty(
                vec![0.1],
                vec![0.1],
                |_| Ok(text("")),
                |_| Err(EngineError::Failed("model missing".into()))
            ),
            Err(EngineError::Failed(_))
        ));
    }

    #[test]
    fn ordinary_speech_including_clipped_waveforms_is_preserved() {
        let pcm: Vec<f32> = (0..16_000)
            .map(|i| {
                (i as f32 * 2.0 * std::f32::consts::PI * 200.0 / 16_000.0)
                    .sin()
                    .signum()
            })
            .collect();
        assert_eq!(clean_startup_dc(&pcm), pcm);
    }

    #[test]
    fn removes_sustained_dc_without_cutting_or_muting_subsequent_speech() {
        let mut pcm = vec![1.0; 3_200];
        pcm.extend(
            (0..16_000)
                .map(|i| 0.2 * (i as f32 * 2.0 * std::f32::consts::PI * 200.0 / 16_000.0).sin()),
        );
        let cleaned = clean_startup_dc(&pcm);
        assert_eq!(cleaned.len(), pcm.len());
        assert!(cleaned.iter().all(|x| (-1.0..=1.0).contains(x)));
        assert!(cleaned[..3_200].iter().all(|x| x.abs() < 0.001));
        let input_energy: f32 = pcm[8_000..].iter().map(|x| x * x).sum();
        let output_energy: f32 = cleaned[8_000..].iter().map(|x| x * x).sum();
        assert!(output_energy / input_energy > 0.95);
    }
}
