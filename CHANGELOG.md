# Changelog

## 0.1.0 (unreleased)

First version, following the v3 API spec: `convert` in one call; jobs (create, get, list, all, wait, cancel,
delete); file upload and download; operations; `me`; webhook signature checks; typed errors; retries that never
create a job twice. Async (tokio + reqwest, rustls).
