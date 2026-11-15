use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use hmac::{Hmac, Mac};
use runconvert::{tasks, webhooks, Client, ConvertOptions, Error, ListParams};
use serde_json::{json, Value};
use sha2::Sha256;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn ok(data: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "success": true, "data": data }))
}

fn fail(status: u16, code: &str, msg: &str) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .set_body_json(json!({ "success": false, "error": { "code": code, "message": msg } }))
}

fn client(server: &MockServer) -> Client {
    Client::builder()
        .api_key("rc_live_test")
        .base_url(&format!("{}/v3", server.uri()))
        .max_retries(2)
        .fast_waits_for_tests()
        .build()
}

/// Replies by call number.
struct Seq<F: Fn(usize, &Request) -> ResponseTemplate + Send + Sync>(AtomicUsize, F);
impl<F: Fn(usize, &Request) -> ResponseTemplate + Send + Sync> Respond for Seq<F> {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        (self.1)(n, req)
    }
}
fn seq<F: Fn(usize, &Request) -> ResponseTemplate + Send + Sync + 'static>(f: F) -> Seq<F> {
    Seq(AtomicUsize::new(0), f)
}

fn query(req: &Request, key: &str) -> Option<String> {
    req.url
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

#[tokio::test]
async fn sends_key_and_unwraps_envelope() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/me"))
        .respond_with(ok(json!({ "credits": { "total_remaining": 42 } })))
        .mount(&server)
        .await;
    let me = client(&server).me().await.unwrap();
    assert_eq!(me["credits"]["total_remaining"], 42);
    let req = &server.received_requests().await.unwrap()[0];
    assert_eq!(
        req.headers.get("authorization").unwrap(),
        "Bearer rc_live_test"
    );
    assert!(req
        .headers
        .get("user-agent")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("runconvert-rust/"));
}

#[tokio::test]
async fn api_errors_are_typed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(fail(
            422,
            "BILLING_INSUFFICIENT_CREDITS",
            "Insufficient credits.",
        ))
        .mount(&server)
        .await;
    match client(&server).get_job("job_1").await {
        Err(Error::Api {
            status,
            code,
            message,
        }) => assert_eq!(
            (status, code.as_str(), message.as_str()),
            (422, "BILLING_INSUFFICIENT_CREDITS", "Insufficient credits.")
        ),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn retries_get_but_never_duplicates_a_job() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(seq(|n, _| {
            if n < 3 {
                ResponseTemplate::new(503)
            } else {
                ok(json!({ "id": "job_1", "status": "finished" }))
            }
        }))
        .mount(&server)
        .await;
    assert_eq!(
        client(&server).get_job("job_1").await.unwrap().status,
        "finished"
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 3);

    let s2 = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(fail(500, "INTERNAL", "boom"))
        .mount(&s2)
        .await;
    assert!(client(&s2).create_job(BTreeMap::new(), None).await.is_err());
    assert_eq!(s2.received_requests().await.unwrap().len(), 1);

    let s3 = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(seq(|n, _| {
            if n == 1 {
                fail(429, "ACTIVE_JOB_LIMIT_REACHED", "x")
            } else {
                ok(json!({ "id": "job_1", "status": "waiting" }))
            }
        }))
        .mount(&s3)
        .await;
    assert_eq!(
        client(&s3)
            .create_job(BTreeMap::new(), None)
            .await
            .unwrap()
            .id,
        "job_1"
    );
    let reqs = s3.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 2);
    assert_eq!(query(&reqs[1], "include").as_deref(), Some("tasks"));
}

#[tokio::test]
async fn list_and_all() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(seq(|_, req| {
            let p = query(req, "page").unwrap_or_default();
            ResponseTemplate::new(200).set_body_json(json!({ "success": true, "data": [{ "id": format!("job_p{p}"), "status": "finished" }], "meta": { "page": p.parse::<u64>().unwrap_or(0), "total_pages": 2 } }))
        }))
        .mount(&server)
        .await;
    let rc = client(&server);
    let page = rc
        .list_jobs(&ListParams {
            status: Some("finished".into()),
            tag: Some("x".into()),
            page: Some(1),
            per_page: Some(1),
            include_tasks: false,
        })
        .await
        .unwrap();
    assert_eq!(page.total_pages, 2);
    let first = &server.received_requests().await.unwrap()[0];
    assert_eq!(query(first, "filter[status]").as_deref(), Some("finished"));
    assert_eq!(query(first, "filter[tag]").as_deref(), Some("x"));
    let ids: Vec<String> = rc
        .all_jobs(ListParams {
            per_page: Some(1),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_iter()
        .map(|j| j.id)
        .collect();
    assert_eq!(ids, vec!["job_p1", "job_p2"]);
}

#[tokio::test]
async fn wait_finished_failed_and_timeout() {
    let server = MockServer::start().await;
    let states = ["waiting", "processing", "finished"];
    Mock::given(method("GET"))
        .respond_with(seq(move |n, _| {
            ok(json!({ "id": "job_1", "status": states[(n - 1).min(2)] }))
        }))
        .mount(&server)
        .await;
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let s = seen.clone();
    let f = move |j: &runconvert::Job| s.lock().unwrap().push(j.status.clone());
    assert_eq!(
        client(&server)
            .wait("job_1", None, Some(&f))
            .await
            .unwrap()
            .status,
        "finished"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["waiting", "processing", "finished"]
    );

    let s2 = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ok(json!({ "id": "job_1", "status": "error", "tasks": [{ "name": "c", "operation": "convert", "status": "error", "code": "INVALID_DATA", "reason": "INPUT_EMPTY", "message": "The file is empty." }] })))
        .mount(&s2)
        .await;
    match client(&s2).wait("job_1", None, None).await {
        Err(e @ Error::JobFailed { .. }) => {
            assert_eq!(e.to_string(), "The file is empty.");
            if let Error::JobFailed { task, .. } = e {
                assert_eq!(task.unwrap().reason.as_deref(), Some("INPUT_EMPTY"));
            }
        }
        other => panic!("{other:?}"),
    }

    let s3 = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ok(json!({ "id": "job_1", "status": "processing" })))
        .mount(&s3)
        .await;
    assert!(matches!(
        client(&s3)
            .wait("job_1", Some(std::time::Duration::from_millis(1)), None)
            .await,
        Err(Error::WaitTimeout { .. })
    ));
}

#[tokio::test]
async fn delete_and_operations() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ok(json!({ "engines": [] })))
        .mount(&server)
        .await;
    let rc = client(&server);
    rc.delete_job("job_1").await.unwrap();
    rc.operations_for_pair("MP4", "MP3").await.unwrap();
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(
        (
            query(&reqs[1], "input").as_deref(),
            query(&reqs[1], "output").as_deref()
        ),
        (Some("mp4"), Some("mp3"))
    );
}

#[test]
fn webhook_signatures() {
    let body = br#"{"event":"job.finished","job":{"id":"job_1"}}"#;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"whsec_x").unwrap();
    mac.update(body);
    let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    assert!(webhooks::verify(body, Some(&sig), "whsec_x"));
    let mut tampered = body.to_vec();
    tampered.push(b' ');
    assert!(!webhooks::verify(&tampered, Some(&sig), "whsec_x"));
    assert!(!webhooks::verify(body, Some(&sig), "other"));
    assert!(!webhooks::verify(body, Some("sha256=nothex"), "whsec_x"));
    assert!(!webhooks::verify(body, None, "whsec_x"));
    assert_eq!(
        webhooks::construct_event(body, Some(&sig), "whsec_x").unwrap()["job"]["id"],
        "job_1"
    );
    assert!(webhooks::construct_event(
        body,
        Some(&format!("sha256={}", "0".repeat(64))),
        "whsec_x"
    )
    .is_err());
}

#[tokio::test]
async fn convert_uploads_waits_and_downloads() {
    let server = MockServer::start().await;
    let base = server.uri();
    let b1 = base.clone();
    Mock::given(method("POST")).and(path("/v3/jobs"))
        .respond_with(move |_: &Request| ok(json!({ "id": "job_1", "status": "waiting", "tasks": [{ "name": "import-1", "operation": "import/direct/upload", "status": "waiting", "upload_url": format!("{b1}/upload?token=t") }] })))
        .mount(&server).await;
    Mock::given(method("PUT"))
        .and(path("/upload"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dl/notes.pdf"))
        .respond_with(ResponseTemplate::new(200).set_body_string("PDF"))
        .mount(&server)
        .await;
    let b2 = base.clone();
    Mock::given(method("GET")).and(path("/v3/jobs/job_1"))
        .respond_with(seq(move |n, _| ok(json!({ "id": "job_1", "status": if n > 1 { "finished" } else { "processing" }, "tasks": [{ "name": "export-1", "operation": "export/url", "status": "finished", "result": { "files": [{ "filename": "notes.pdf", "url": format!("{b2}/dl/notes.pdf") }] } }] }))))
        .mount(&server).await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("notes.txt");
    std::fs::write(&src, "hello").unwrap();
    let mut opts = serde_json::Map::new();
    opts.insert("page_size".into(), "a4".into());
    let res = client(&server)
        .convert(
            src.to_str().unwrap(),
            ConvertOptions::to("pdf")
                .out_dir(dir.path().join("out"))
                .options(opts),
        )
        .await
        .unwrap();
    let reqs = server.received_requests().await.unwrap();
    let created: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(
        created["tasks"]["convert-1"],
        json!({ "page_size": "a4", "operation": "convert", "input": "import-1", "input_format": "txt", "output_format": "pdf" })
    );
    let put = reqs.iter().find(|r| r.method.as_str() == "PUT").unwrap();
    assert_eq!(
        (
            query(put, "filename").as_deref(),
            query(put, "token").as_deref(),
            put.body.as_slice()
        ),
        (Some("notes.txt"), Some("t"), b"hello".as_slice())
    );
    assert_eq!(std::fs::read_to_string(&res.saved[0]).unwrap(), "PDF");
}

#[tokio::test]
async fn convert_from_url_imports_it() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ok(json!({ "id": "job_2", "status": "waiting" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ok(json!({ "id": "job_2", "status": "finished" })))
        .mount(&server)
        .await;
    client(&server)
        .convert(
            "https://example.com/a/talk.MP4?x=1",
            ConvertOptions::to("mp3"),
        )
        .await
        .unwrap();
    let body: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert_eq!(
        body["tasks"]["import-1"],
        tasks::import_url("https://example.com/a/talk.MP4?x=1")
    );
    assert_eq!(body["tasks"]["convert-1"]["input_format"], "mp4");
}
