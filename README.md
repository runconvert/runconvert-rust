# RunConvert SDK for Rust

Convert files between 240+ formats (3,000+ conversions) with the [RunConvert API](https://www.runconvert.com/docs):
documents, images, video, audio, ebooks, archives, fonts and CAD. Async (tokio + reqwest with rustls).

```toml
[dependencies]
runconvert = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use runconvert::{Client, ConvertOptions};

#[tokio::main]
async fn main() -> Result<(), runconvert::Error> {
    let rc = Client::new(None); // reads RUNCONVERT_API_KEY

    // One call: upload, convert, wait, download.
    let res = rc.convert("report.docx", ConvertOptions::to("pdf").out_dir("out")).await?;
    println!("{:?}", res.saved); // ["out/report.pdf"]
    Ok(())
}
```

Get an API key in **Dashboard → API Keys** on [runconvert.com](https://www.runconvert.com). Keep keys on the server.

## Convert

```rust
// From a URL (imported by RunConvert, nothing to upload)
let mut opts = serde_json::Map::new();
opts.insert("audioBitrate".into(), "192k".into());
rc.convert("https://example.com/talk.mp4", ConvertOptions::to("mp3").options(opts)).await?;

// Progress and a time limit
rc.convert("movie.mov", ConvertOptions::to("mp4").out_dir("out")
    .timeout(std::time::Duration::from_secs(600))
    .on_update(|job| println!("{}", job.status))).await?;
```

## Jobs with your own tasks

```rust
use std::collections::BTreeMap;
use runconvert::tasks;

// Merge two PDFs
let mut t = BTreeMap::new();
t.insert("a".to_string(), tasks::import_url("https://example.com/cover.pdf"));
t.insert("b".to_string(), tasks::import_url("https://example.com/body.pdf"));
t.insert("merge".to_string(), tasks::convert("pdf", "pdf", serde_json::json!(["a", "b"]), None));
t.insert("export".to_string(), tasks::export_url("merge"));
let job = rc.create_job(t, Some("merge")).await?;
let job = rc.wait(&job.id, None, None).await?;
for f in job.exported_files() {
    rc.download(&f, "out").await?;
}
```

| Method | What it does |
|---|---|
| `create_job / get_job / list_jobs / all_jobs / wait / cancel_job / delete_job` | Jobs, with paging and polling |
| `upload(url, path, name)` / `download(file, dir)` | Transfers |
| `operations(category) / operations_for_input("mp4") / operations_for_pair("mp4", "mp3")` | Formats, targets, engines (no key needed) |
| `me()` | Account, team and credit balance |

## Webhooks

```rust
let event = runconvert::webhooks::construct_event(&body_bytes, signature_header, &secret)?;
if event["event"] == "job.finished" { /* … */ }
```

## Errors

`runconvert::Error` is `Api { status, code, message }` (the API said no), `JobFailed { job, task }`
(the job ended in `error`; `task.code`, `task.reason`, `task.message` say why), `WaitTimeout`, `Transport` or
`Invalid`. Network errors, `429` and `5xx` are retried twice with backoff; creating a job is only retried when
the request never reached the API, so a job is never created twice.

## Development

```bash
cargo test --test unit                                          # no network
RUNCONVERT_API_KEY=rc_live_… cargo test --test live -- --nocapture   # real API, about 2 credits
```

## License

MIT
