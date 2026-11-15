//! Official RunConvert SDK for Rust: convert files between 240+ formats.
//!
//! ```no_run
//! # async fn run() -> Result<(), runconvert::Error> {
//! let rc = runconvert::Client::new(None); // reads RUNCONVERT_API_KEY
//! let res = rc.convert("report.docx", runconvert::ConvertOptions::to("pdf").out_dir("out")).await?;
//! println!("{:?}", res.saved);
//! # Ok(()) }
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::Sha256;
use tokio::io::AsyncWriteExt;

/// Version of this SDK.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The production API.
pub const DEFAULT_BASE_URL: &str = "https://api.runconvert.com/v3";
const RETRY_STATUS: [u16; 7] = [408, 425, 429, 500, 502, 503, 504];

// ---------------------------------------------------------------- errors

/// Everything that can go wrong.
#[derive(Debug)]
pub enum Error {
    /// The API said no: `{"success": false, "error": {"code", "message"}}`. New codes can appear;
    /// branch on `status` for unknown ones.
    Api {
        status: u16,
        code: String,
        message: String,
    },
    /// The job ended with status `error`; `task` is the first failed task (code, reason, message).
    JobFailed {
        job: Box<Job>,
        task: Option<Box<Task>>,
    },
    /// `wait` gave up before the job reached `finished` or `error`.
    WaitTimeout { job: Box<Job> },
    /// No HTTP response (network), or a local file problem.
    Transport(String),
    /// Bad input to the SDK (e.g. an unknown source format).
    Invalid(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Api {
                status,
                code,
                message,
            } => write!(f, "{message} ({code}, HTTP {status})"),
            Error::JobFailed { job, task } => match task
                .as_ref()
                .and_then(|t| t.message.clone())
                .filter(|m| !m.is_empty())
            {
                Some(m) => write!(f, "{m}"),
                None => write!(f, "job {} failed", job.id),
            },
            Error::WaitTimeout { job } => write!(f, "job {} still {}", job.id, job.status),
            Error::Transport(m) | Error::Invalid(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// The API error code (e.g. `BILLING_INSUFFICIENT_CREDITS`), when there is one.
    pub fn code(&self) -> Option<&str> {
        match self {
            Error::Api { code, .. } if !code.is_empty() => Some(code),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------- models

/// A result file: name, temporary download link and size.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResultFile {
    pub filename: String,
    /// Temporary download link.
    pub url: String,
    #[serde(default)]
    pub size: Option<u64>,
}

/// A step of a job. On error, `code` is the category and `reason` the exact, stable cause.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Task {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub operation: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub percent: Option<u32>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub credits: Option<i64>,
    /// `import/direct/upload` only: PUT the file here (see [`Client::upload`]).
    #[serde(default)]
    pub upload_url: Option<String>,
    #[serde(default)]
    pub upload_deadline: Option<String>,
    #[serde(default)]
    pub result: Option<TaskResult>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub reason_params: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TaskResult {
    #[serde(default)]
    pub files: Vec<ResultFile>,
}

/// A job: named tasks that run in order (import → convert → export).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Job {
    pub id: String,
    /// waiting | processing | finished | error
    pub status: String,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub credits_used: Option<i64>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub ended_at: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub tasks: Vec<Task>,
}

impl Job {
    /// The task with this name.
    pub fn task(&self, name: &str) -> Option<&Task> {
        self.tasks.iter().find(|t| t.name == name)
    }

    /// The files produced by the job's `export/url` tasks.
    pub fn exported_files(&self) -> Vec<ResultFile> {
        self.tasks
            .iter()
            .filter(|t| t.operation == "export/url")
            .flat_map(|t| {
                t.result
                    .as_ref()
                    .map(|r| r.files.clone())
                    .unwrap_or_default()
            })
            .collect()
    }
}

/// One page of jobs.
#[derive(Debug, Clone)]
pub struct JobPage {
    pub data: Vec<Job>,
    pub page: u64,
    pub total_pages: u64,
    pub total: u64,
}

/// Builders for task definitions (JSON objects, so any documented option can be added).
pub mod tasks {
    use serde_json::{json, Map, Value};

    /// Imports a file from a public http(s) URL.
    pub fn import_url(source_url: &str) -> Value {
        json!({ "operation": "import/url", "source_url": source_url })
    }

    /// An upload task; the created task carries `upload_url`.
    pub fn import_upload() -> Value {
        json!({ "operation": "import/direct/upload" })
    }

    /// Converts `input` (a task name, or an array of names to merge/combine). `options` are engine settings.
    pub fn convert(
        input_format: &str,
        output_format: &str,
        input: impl Into<Value>,
        options: Option<Map<String, Value>>,
    ) -> Value {
        let mut t = options.unwrap_or_default();
        t.insert("operation".into(), "convert".into());
        t.insert("input".into(), input.into());
        t.insert("input_format".into(), input_format.into());
        t.insert("output_format".into(), output_format.into());
        Value::Object(t)
    }

    /// Temporary download links for `input` (a task name or an array of names).
    pub fn export_url(input: impl Into<Value>) -> Value {
        json!({ "operation": "export/url", "input": input.into() })
    }
}

/// A progress callback for [`Client::convert`].
pub type OnUpdate = Box<dyn Fn(&Job) + Send + Sync>;

/// Options for [`Client::convert`].
#[derive(Default)]
pub struct ConvertOptions {
    to: String,
    from: Option<String>,
    options: Option<Map<String, Value>>,
    out_dir: Option<PathBuf>,
    tag: Option<String>,
    timeout: Option<Duration>,
    on_update: Option<OnUpdate>,
}

impl ConvertOptions {
    /// The target format, e.g. `"mp3"`.
    pub fn to(format: &str) -> Self {
        Self {
            to: format.trim_start_matches('.').to_lowercase(),
            ..Default::default()
        }
    }
    /// The source format; read from the file name or URL when not set.
    pub fn from(mut self, format: &str) -> Self {
        self.from = Some(format.to_lowercase());
        self
    }
    /// Engine options, e.g. `{"audioBitrate": "192k"}` (see [`Client::operations_for_pair`]).
    pub fn options(mut self, options: Map<String, Value>) -> Self {
        self.options = Some(options);
        self
    }
    /// Save results here (created if needed). Without it, nothing is downloaded.
    pub fn out_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.out_dir = Some(dir.into());
        self
    }
    pub fn tag(mut self, tag: &str) -> Self {
        self.tag = Some(tag.into());
        self
    }
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    pub fn on_update(mut self, f: impl Fn(&Job) + Send + Sync + 'static) -> Self {
        self.on_update = Some(Box::new(f));
        self
    }
}

/// Result of [`Client::convert`].
#[derive(Debug, Clone)]
pub struct ConvertResult {
    pub job: Job,
    /// The converted files, with temporary download links.
    pub files: Vec<ResultFile>,
    /// Local paths, when `out_dir` was set.
    pub saved: Vec<PathBuf>,
}

/// Filters for [`Client::list_jobs`].
#[derive(Debug, Clone, Default)]
pub struct ListParams {
    pub status: Option<String>,
    pub tag: Option<String>,
    pub page: Option<u64>,
    pub per_page: Option<u64>,
    pub include_tasks: bool,
}

// ---------------------------------------------------------------- client

/// The RunConvert API client. Cheap to clone; reuse it.
#[derive(Clone)]
pub struct Client {
    api_key: String,
    base_url: String,
    max_retries: u32,
    timeout: Duration,
    http: reqwest::Client,
    user_agent: String,
    /// Shortened waits (tests).
    fast: bool,
}

impl Client {
    /// `api_key: None` reads `RUNCONVERT_API_KEY`.
    pub fn new(api_key: Option<&str>) -> Self {
        Self::builder().api_key_opt(api_key).build()
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Sends a request and unwraps `{"success": true, "data": …, "meta": …}`.
    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
        query: &[(&str, String)],
    ) -> Result<(Value, Value), Error> {
        // POST /jobs is only repeated when it never reached the server: a job is never created twice.
        let idempotent = method != reqwest::Method::POST || path.ends_with("/cancel");
        let mut attempt = 0;
        loop {
            let mut req = self
                .http
                .request(method.clone(), self.url(path))
                .timeout(self.timeout)
                .header("Accept", "application/json")
                .header("User-Agent", &self.user_agent)
                .query(
                    &query
                        .iter()
                        .filter(|(_, v)| !v.is_empty())
                        .collect::<Vec<_>>(),
                );
            if !self.api_key.is_empty() {
                req = req.bearer_auth(&self.api_key);
            }
            if let Some(b) = body {
                req = req.json(b);
            }
            let res = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let reached = e.is_timeout();
                    if attempt < self.max_retries && (idempotent || !reached) {
                        self.pause(backoff(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(Error::Transport(format!(
                        "request to RunConvert failed: {e}"
                    )));
                }
            };
            let status = res.status().as_u16();
            if RETRY_STATUS.contains(&status)
                && attempt < self.max_retries
                && (idempotent || status == 429)
            {
                let wait = res
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .filter(|s| *s > 0)
                    .map(|s| Duration::from_secs(s.min(30)))
                    .unwrap_or_else(|| backoff(attempt));
                self.pause(wait).await;
                attempt += 1;
                continue;
            }
            if status == 204 {
                return Ok((Value::Null, Value::Null));
            }
            let text = res
                .text()
                .await
                .map_err(|e| Error::Transport(e.to_string()))?;
            let root: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            if status >= 400 || root.get("success") == Some(&Value::Bool(false)) {
                let err = root.get("error");
                return Err(Error::Api {
                    status,
                    code: err
                        .and_then(|e| e.get("code"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    message: err
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("RunConvert API error (HTTP {status})")),
                });
            }
            let data = root.get("data").cloned().unwrap_or(Value::Null);
            let meta = root.get("meta").cloned().unwrap_or(Value::Null);
            return Ok((data, meta));
        }
    }

    async fn pause(&self, d: Duration) {
        tokio::time::sleep(if self.fast {
            Duration::from_millis(1)
        } else {
            d
        })
        .await;
    }

    fn parse<T: for<'de> Deserialize<'de>>(v: Value) -> Result<T, Error> {
        serde_json::from_value(v).map_err(|e| Error::Transport(format!("unexpected response: {e}")))
    }

    // ---- account ----

    /// The account, team and credit balance behind the API key.
    pub async fn me(&self) -> Result<Value, Error> {
        Ok(self
            .request(reqwest::Method::GET, "/me", None, &[])
            .await?
            .0)
    }

    // ---- jobs ----

    /// Creates a job from named tasks (see [`tasks`]). Returned with its tasks.
    pub async fn create_job(
        &self,
        tasks: BTreeMap<String, Value>,
        tag: Option<&str>,
    ) -> Result<Job, Error> {
        let mut body = json!({ "tasks": tasks });
        if let Some(t) = tag {
            body["tag"] = t.into();
        }
        Self::parse(
            self.request(
                reqwest::Method::POST,
                "/jobs",
                Some(&body),
                &[("include", "tasks".into())],
            )
            .await?
            .0,
        )
    }

    /// A job with its tasks.
    pub async fn get_job(&self, id: &str) -> Result<Job, Error> {
        let path = format!("/jobs/{}", urlencode(id));
        Self::parse(
            self.request(
                reqwest::Method::GET,
                &path,
                None,
                &[("include", "tasks".into())],
            )
            .await?
            .0,
        )
    }

    /// One page of jobs, newest first.
    pub async fn list_jobs(&self, p: &ListParams) -> Result<JobPage, Error> {
        let q = [
            ("filter[status]", p.status.clone().unwrap_or_default()),
            ("filter[tag]", p.tag.clone().unwrap_or_default()),
            ("page", p.page.map(|v| v.to_string()).unwrap_or_default()),
            (
                "per_page",
                p.per_page.map(|v| v.to_string()).unwrap_or_default(),
            ),
            (
                "include",
                if p.include_tasks {
                    "tasks".into()
                } else {
                    String::new()
                },
            ),
        ];
        let (data, meta) = self
            .request(reqwest::Method::GET, "/jobs", None, &q)
            .await?;
        let n = |k: &str| meta.get(k).and_then(Value::as_u64).unwrap_or(0);
        Ok(JobPage {
            data: if data.is_null() {
                vec![]
            } else {
                Self::parse(data)?
            },
            page: n("page"),
            total_pages: n("total_pages"),
            total: n("total"),
        })
    }

    /// Every job, page by page.
    pub async fn all_jobs(&self, mut p: ListParams) -> Result<Vec<Job>, Error> {
        let mut out = Vec::new();
        let mut page = 1;
        loop {
            p.page = Some(page);
            let res = self.list_jobs(&p).await?;
            let done = res.data.is_empty() || (res.total_pages > 0 && page >= res.total_pages);
            out.extend(res.data);
            if done {
                return Ok(out);
            }
            page += 1;
        }
    }

    /// Cancels pending tasks; credits for unfinished tasks are refunded.
    pub async fn cancel_job(&self, id: &str) -> Result<Job, Error> {
        let path = format!("/jobs/{}/cancel", urlencode(id));
        Self::parse(
            self.request(reqwest::Method::POST, &path, None, &[])
                .await?
                .0,
        )
    }

    /// Deletes a job.
    pub async fn delete_job(&self, id: &str) -> Result<(), Error> {
        let path = format!("/jobs/{}", urlencode(id));
        self.request(reqwest::Method::DELETE, &path, None, &[])
            .await
            .map(|_| ())
    }

    /// Polls until `finished` or `error` (0.7 s, slowing to 3 s). A failed job is `Error::JobFailed`.
    pub async fn wait(
        &self,
        id: &str,
        timeout: Option<Duration>,
        on_update: Option<&(dyn Fn(&Job) + Send + Sync)>,
    ) -> Result<Job, Error> {
        let started = Instant::now();
        let mut delay = Duration::from_millis(700);
        loop {
            let job = self.get_job(id).await?;
            if let Some(f) = on_update {
                f(&job);
            }
            match job.status.as_str() {
                "finished" => return Ok(job),
                "error" => {
                    let task = job
                        .tasks
                        .iter()
                        .find(|t| t.status == "error")
                        .cloned()
                        .map(Box::new);
                    return Err(Error::JobFailed {
                        job: Box::new(job),
                        task,
                    });
                }
                _ => {}
            }
            if let Some(t) = timeout {
                if started.elapsed() + delay > t {
                    return Err(Error::WaitTimeout { job: Box::new(job) });
                }
            }
            self.pause(delay).await;
            delay = (delay + Duration::from_millis(300)).min(Duration::from_secs(3));
        }
    }

    // ---- operations (public, no key needed) ----

    /// Every input format with its targets: `{"categories": [...], "formats": [...]}`.
    pub async fn operations(&self, category: Option<&str>) -> Result<Value, Error> {
        Ok(self
            .request(
                reqwest::Method::GET,
                "/operations",
                None,
                &[("category", category.unwrap_or_default().into())],
            )
            .await?
            .0)
    }

    /// What one format converts to, with credit costs.
    pub async fn operations_for_input(&self, input: &str) -> Result<Value, Error> {
        Ok(self
            .request(
                reqwest::Method::GET,
                "/operations",
                None,
                &[("input", input.to_lowercase())],
            )
            .await?
            .0)
    }

    /// Engines for one conversion and the options a convert task accepts.
    pub async fn operations_for_pair(&self, input: &str, output: &str) -> Result<Value, Error> {
        Ok(self
            .request(
                reqwest::Method::GET,
                "/operations",
                None,
                &[
                    ("input", input.to_lowercase()),
                    ("output", output.to_lowercase()),
                ],
            )
            .await?
            .0)
    }

    // ---- files ----

    /// PUTs a local file to a task's upload URL before its deadline; `filename` keeps its name.
    pub async fn upload(
        &self,
        upload_url: &str,
        path: impl AsRef<Path>,
        filename: Option<&str>,
    ) -> Result<(), Error> {
        let path = path.as_ref();
        let name = filename
            .map(str::to_string)
            .or_else(|| path.file_name().map(|n| n.to_string_lossy().into_owned()));
        let mut url = url::Url::parse(upload_url)
            .map_err(|e| Error::Invalid(format!("bad upload URL: {e}")))?;
        if let Some(n) = &name {
            url.query_pairs_mut().append_pair("filename", n);
        }
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|e| Error::Transport(format!("{}: {e}", path.display())))?;
        let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let res = self
            .http
            .put(url)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", len)
            .header("User-Agent", &self.user_agent)
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Transport(format!("upload failed: {e}")))?;
        let status = res.status().as_u16();
        if status >= 300 {
            let text = res.text().await.unwrap_or_default();
            return Err(Error::Api {
                status,
                code: String::new(),
                message: format!(
                    "upload failed (HTTP {status}): {}",
                    text.chars().take(300).collect::<String>()
                ),
            });
        }
        Ok(())
    }

    /// Saves a result file into `dir` (created if needed), keeping its name. Returns the path.
    pub async fn download(
        &self,
        file: &ResultFile,
        dir: impl AsRef<Path>,
    ) -> Result<PathBuf, Error> {
        let dir = dir.as_ref();
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        let name = Path::new(&file.filename)
            .file_name()
            .map(|n| n.to_owned())
            .unwrap_or_else(|| "download".into());
        let dest = dir.join(name);
        let res = self
            .http
            .get(&file.url)
            .header("User-Agent", &self.user_agent)
            .send()
            .await
            .map_err(|e| Error::Transport(format!("download failed: {e}")))?;
        let status = res.status().as_u16();
        if status >= 300 {
            return Err(Error::Api {
                status,
                code: String::new(),
                message: format!("download failed (HTTP {status})"),
            });
        }
        let mut out = tokio::fs::File::create(&dest)
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        let mut stream = res.bytes_stream();
        while let Some(chunk) = stream.next().await {
            out.write_all(&chunk.map_err(|e| Error::Transport(e.to_string()))?)
                .await
                .map_err(|e| Error::Transport(e.to_string()))?;
        }
        out.flush()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        Ok(dest)
    }

    // ---- convert ----

    /// Converts one file in one call: upload a local path (or import an http(s) URL), convert, wait,
    /// and download into `out_dir` when set.
    pub async fn convert(
        &self,
        file_or_url: &str,
        o: ConvertOptions,
    ) -> Result<ConvertResult, Error> {
        let is_url = file_or_url.starts_with("http://") || file_or_url.starts_with("https://");
        let name = if is_url {
            url::Url::parse(file_or_url)
                .ok()
                .and_then(|u| {
                    u.path_segments()
                        .and_then(|mut s| s.next_back().map(str::to_string))
                })
                .unwrap_or_default()
        } else {
            Path::new(file_or_url)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        };
        let from = o.from.clone().unwrap_or_else(|| {
            Path::new(&name)
                .extension()
                .map(|e| e.to_string_lossy().to_lowercase())
                .unwrap_or_default()
        });
        if from.is_empty() {
            return Err(Error::Invalid(
                "can't tell the source format: set ConvertOptions::from(\"mp4\")".into(),
            ));
        }
        let mut job_tasks = BTreeMap::new();
        job_tasks.insert(
            "import-1".into(),
            if is_url {
                tasks::import_url(file_or_url)
            } else {
                tasks::import_upload()
            },
        );
        job_tasks.insert(
            "convert-1".into(),
            tasks::convert(&from, &o.to, "import-1", o.options.clone()),
        );
        job_tasks.insert("export-1".into(), tasks::export_url("convert-1"));
        let job = self.create_job(job_tasks, o.tag.as_deref()).await?;
        if !is_url {
            let upload_url = job
                .task("import-1")
                .and_then(|t| t.upload_url.clone())
                .ok_or_else(|| Error::Transport("the job has no upload URL".into()))?;
            self.upload(&upload_url, file_or_url, Some(&name)).await?;
        }
        let job = self
            .wait(&job.id, o.timeout, o.on_update.as_deref())
            .await?;
        let files = job.exported_files();
        let mut saved = Vec::new();
        if let Some(dir) = &o.out_dir {
            for f in &files {
                saved.push(self.download(f, dir).await?);
            }
        }
        Ok(ConvertResult { job, files, saved })
    }
}

/// Options for [`Client`].
#[derive(Default)]
pub struct ClientBuilder {
    api_key: Option<String>,
    base_url: Option<String>,
    timeout: Option<Duration>,
    max_retries: Option<u32>,
    http: Option<reqwest::Client>,
    fast: bool,
}

impl ClientBuilder {
    /// Defaults to the `RUNCONVERT_API_KEY` environment variable.
    pub fn api_key(mut self, key: &str) -> Self {
        self.api_key = Some(key.into());
        self
    }
    fn api_key_opt(mut self, key: Option<&str>) -> Self {
        self.api_key = key.map(str::to_string);
        self
    }
    pub fn base_url(mut self, url: &str) -> Self {
        self.base_url = Some(url.trim_end_matches('/').into());
        self
    }
    /// Per API request (uploads and downloads have none). Default 60 s.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }
    /// Retries for network errors, 429 and 5xx. Default 2.
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = Some(n);
        self
    }
    pub fn http_client(mut self, c: reqwest::Client) -> Self {
        self.http = Some(c);
        self
    }
    #[doc(hidden)]
    pub fn fast_waits_for_tests(mut self) -> Self {
        self.fast = true;
        self
    }
    pub fn build(self) -> Client {
        Client {
            api_key: self
                .api_key
                .unwrap_or_else(|| std::env::var("RUNCONVERT_API_KEY").unwrap_or_default()),
            base_url: self.base_url.unwrap_or_else(|| DEFAULT_BASE_URL.into()),
            max_retries: self.max_retries.unwrap_or(2),
            timeout: self.timeout.unwrap_or(Duration::from_secs(60)),
            http: self.http.unwrap_or_default(),
            user_agent: format!("runconvert-rust/{VERSION}"),
            fast: self.fast,
        }
    }
}

fn backoff(attempt: u32) -> Duration {
    let ms = (500.0 * 2f64.powi(attempt as i32)).min(8000.0) * (0.75 + rand::random::<f64>() / 2.0);
    Duration::from_millis(ms as u64)
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

// ---------------------------------------------------------------- webhooks

/// Webhook signature checks. Every delivery has an `X-RunConvert-Signature` header: `sha256=` + hex
/// HMAC-SHA256 of the raw request body, keyed with the endpoint's secret.
pub mod webhooks {
    use super::*;

    /// True when the signature matches the raw body (pass it exactly as received).
    pub fn verify(raw_body: &[u8], signature: Option<&str>, secret: &str) -> bool {
        let Some(sig) = signature else { return false };
        if secret.is_empty() {
            return false;
        }
        let given = sig.trim().to_lowercase();
        let given = given.strip_prefix("sha256=").unwrap_or(&given);
        let Ok(bytes) = hex::decode(given) else {
            return false;
        };
        if bytes.len() != 32 {
            return false;
        }
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
        mac.update(raw_body);
        mac.verify_slice(&bytes).is_ok()
    }

    /// Verifies the signature, then parses the event (`{"event", "job" | "task"}`).
    pub fn construct_event(
        raw_body: &[u8],
        signature: Option<&str>,
        secret: &str,
    ) -> Result<Value, Error> {
        if !verify(raw_body, signature, secret) {
            return Err(Error::Invalid(
                "invalid RunConvert webhook signature".into(),
            ));
        }
        serde_json::from_slice(raw_body)
            .map_err(|e| Error::Invalid(format!("webhook body is not JSON: {e}")))
    }
}
