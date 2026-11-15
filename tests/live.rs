//! Live tests against the real API: RUNCONVERT_API_KEY=… cargo test --test live -- --nocapture
//! (converts one small text file, about 2 credits). Skipped without a key.
use runconvert::{Client, ConvertOptions, Error};

fn has_key() -> bool {
    std::env::var("RUNCONVERT_API_KEY")
        .map(|k| !k.is_empty())
        .unwrap_or(false)
}

#[tokio::test]
async fn live_me_and_operations() {
    if !has_key() {
        return eprintln!("skipped: set RUNCONVERT_API_KEY");
    }
    assert!(Client::new(None)
        .me()
        .await
        .unwrap()
        .get("credits")
        .is_some());
    let public = Client::builder().api_key("").build();
    let txt = public.operations_for_input("TXT").await.unwrap();
    assert!(txt["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|o| o["format"] == "pdf"));
    assert!(
        !public.operations_for_pair("mp4", "mp3").await.unwrap()["engines"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn live_convert_text_to_pdf() {
    if !has_key() {
        return eprintln!("skipped: set RUNCONVERT_API_KEY");
    }
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("sdk-live-test.txt");
    std::fs::write(
        &src,
        "RunConvert Rust SDK live test\nLine two\n\nCafé – 你好 – Привет\n",
    )
    .unwrap();
    let res = Client::new(None)
        .convert(
            src.to_str().unwrap(),
            ConvertOptions::to("pdf")
                .out_dir(dir.path().join("out"))
                .tag("sdk-rust-live-test")
                .timeout(std::time::Duration::from_secs(150)),
        )
        .await
        .unwrap();
    let pdf = std::fs::read(&res.saved[0]).unwrap();
    assert_eq!(&pdf[..5], b"%PDF-");
    println!(
        "  job {}: {}, {} credit(s), {} bytes",
        res.job.id,
        res.job.status,
        res.job.credits_used.unwrap_or(0),
        pdf.len()
    );
}

#[tokio::test]
async fn live_bad_key() {
    if !has_key() {
        return eprintln!("skipped: set RUNCONVERT_API_KEY");
    }
    match Client::builder()
        .api_key("rc_live_invalid")
        .max_retries(0)
        .build()
        .me()
        .await
    {
        Err(Error::Api { status, .. }) => assert_eq!(status, 401),
        other => panic!("{other:?}"),
    }
}
