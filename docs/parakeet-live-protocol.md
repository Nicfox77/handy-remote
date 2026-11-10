# Parakeet remote dictation

Under **Models → Transcription Server**, enable the external server, set its
OpenAI-compatible HTTP base URL (including `/v1`), model ID and optional Bearer
API key. Leave local fallback enabled and select an installed local model.
Existing settings keep HTTP-only behavior and the `whisper-1` model default.

For live dictation, also set **Streaming URL** to the Parakeet live-audio
WebSocket endpoint, for example `ws://your-tailnet-server:18162/v1/audio/stream`.
Use **Overlay → Live** to display previews. Streaming runs for English/Auto;
other selected languages and translation use the HTTP endpoint. The protocol
below is specific to this Parakeet service, rather than OpenAI's Realtime API.
An ordinary OpenAI-compatible HTTP server does not imply streaming support.

The server must support the EOF barrier below. Older servers are rejected and
local fallback handles the recording. The full recording is retained even when
streaming fails, so recovery never depends on the server returning audio.
Dictionary/custom-word corrections apply to finalized text locally; they are
not promised in partial previews. True server decoder word boosting is optional
and is not required or assumed by this client.

## Wire contract

The client connects with the same `Authorization: Bearer ...` header used for
HTTP. Use an authenticated tailnet-only endpoint or TLS endpoint. Credentials
belong in the API-key setting, never inside the URL. The server sends:

```json
{ "type": "ready", "sampleRate": 16000, "dictationFinish": true }
```

The client sends binary mono signed little-endian PCM16 at 16 kHz, in short
frames (up to 200 ms). It preserves pauses. The server may send multiple
utterances during one recording:

```json
{"type":"partial","utteranceId":"id-1","text":"provisional words"}
{"type":"final","utteranceId":"id-1","text":"Complete sentence."}
```

IDs distinguish utterances: two utterances with identical text are both kept,
while repeated finals with the same ID replace that utterance's text.
A partial is never treated as a final result. Server error events or a close
before finalization fail the stream and use local recovery if enabled.

At key release, after all audio frames have been sent, the client sends:

```json
{ "type": "finish" }
```

The server must enqueue this in the same FIFO as audio, flush buffered
separation/denoising and recognition, send all remaining `final` events, and
then send `{"type":"finished"}`. `mute` is not a substitute: it may discard
queued audio. No response is assumed to be complete without this acknowledgement.
The client bounds connection/readiness/write waits to 3 seconds and finalization
to 10 seconds. It checks cancellation throughout and sends periodic `ping`
controls during long recordings. The server answers with `pong`.

An acknowledged empty transcript is checked against local VAD. Speech triggers
local recovery; silence stays empty. A failed connection or stream does not
retry with a speculative partial transcript. The selected local model handles
the original recording, including Canary's empty-result recovery when selected.

## Verification

The Rust transport regression covers confirmed results, silence, incomplete
sample counts, legacy servers, disconnects and cancellation. The optional real
server test uses a public PCM16 fixture containing “quick brown fox” and reads a
key from an owner-only file, without printing it:

```sh
HANDY_TEST_REMOTE_STREAM_URL=ws://your-tailnet-server:18162/v1/audio/stream \
HANDY_TEST_REMOTE_KEY_FILE=/private/path/speech.key \
HANDY_TEST_REMOTE_AUDIO=/public-fixtures/quick-brown-fox.wav \
cargo test --lib managers::transcription::remote::tests::real_parakeet_stream -- --ignored --nocapture
```

The associated speech service adapter forwards this fixed WebSocket path to its
loopback resident backend after authenticating the client. Model loading,
credentials, device placement and existing HTTP routes remain server concerns.
