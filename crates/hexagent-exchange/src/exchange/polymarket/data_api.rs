//! Data API v2 reads. Called only by startup/background account and CLI workers.
//! A scan owns its cursor, deduplication map and buffers; partial or malformed
//! scans never become authoritative empty snapshots. No raw/v1 fallback.
use anyhow::{anyhow, bail, Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const BASE: &str = "https://data-api.polymarket.com";
const MAX_PAGES: usize = 100;
const MAX_ROWS: usize = 50_000;

#[derive(Deserialize)]
pub(crate) struct Page {
    pub data: Vec<Value>,
    pub pagination: Pagination,
}

#[derive(Deserialize)]
pub(crate) struct Pagination {
    pub has_more: bool,
    // Missing is a broken response, not end-of-scan. Explicit null terminates.
    #[serde(deserialize_with = "Option::<String>::deserialize")]
    pub next_cursor: Option<String>,
}

#[derive(Default)]
struct Scan {
    rows: Vec<Value>,
    tokens: HashMap<String, usize>,
    cursors: HashSet<String>,
}

impl Scan {
    fn accept(&mut self, page: Page) -> Result<Option<String>> {
        if page.pagination.has_more != page.pagination.next_cursor.is_some() {
            bail!("Data API has_more/cursor mismatch");
        }
        for mut row in page.data {
            if let Some(condition) = row.get("condition_id").and_then(Value::as_str) {
                if condition.len() == 64 && super::protocol::is_v2_condition(condition) {
                    row["condition_id"] = Value::String(format!("{condition}00"));
                }
            }
            let token = row
                .get("token_id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
                .ok_or_else(|| anyhow!("Data API position missing decimal token_id"))?;
            if let Some(index) = self.tokens.get(token) {
                if self.rows[*index] != row {
                    bail!("Data API changed position within cursor scan: {token}");
                }
                continue;
            }
            if self.rows.len() == MAX_ROWS {
                bail!("Data API position scan exceeded row bound");
            }
            self.tokens.insert(token.to_owned(), self.rows.len());
            self.rows.push(row);
        }
        if let Some(cursor) = page.pagination.next_cursor.as_ref() {
            if cursor.is_empty() || !self.cursors.insert(cursor.clone()) {
                bail!("Data API missing or repeated continuation cursor");
            }
        }
        Ok(page.pagination.next_cursor)
    }
}

pub(crate) fn fetch_positions<T: DeserializeOwned>(wallet: &str) -> Result<Vec<T>> {
    let wallet = wallet.to_owned();
    let rows = crate::async_rt::block_on_runtime(async move {
        fetch_rows(&crate::async_rt::http_client(), BASE, &wallet).await
    })?;
    rows.into_iter()
        .map(|row| serde_json::from_value(row).context("Data API v2 position schema"))
        .collect()
}

async fn fetch_rows(client: &reqwest::Client, base: &str, wallet: &str) -> Result<Vec<Value>> {
    let mut scan = Scan::default();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut url = reqwest::Url::parse(&format!("{}/v2/positions", base.trim_end_matches('/')))?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("user", wallet)
                .append_pair("status", "OPEN")
                .append_pair("include_archived", "true")
                .append_pair("filter_type", "TOKENS")
                .append_pair("filter_amount", "0")
                .append_pair("limit", "500");
            if let Some(value) = cursor.as_ref() {
                query.append_pair("cursor", value);
            }
        }
        let mut attempts = 0;
        let response = loop {
            let response = client
                .get(url.clone())
                .timeout(Duration::from_secs(15))
                .send()
                .await?;
            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS && attempts < 3 {
                let delay = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| {
                        v.parse::<u64>().ok().or_else(|| {
                            chrono::DateTime::parse_from_rfc2822(v).ok().map(|at| {
                                (at.timestamp() - chrono::Utc::now().timestamp()).max(0) as u64
                            })
                        })
                    })
                    .unwrap_or(5);
                // Never retry earlier than the service asks; excessive waits
                // fail the scan and let the existing background owner reschedule.
                if delay > 30 {
                    bail!("Data API throttled; retry after {delay}s");
                }
                tokio::time::sleep(Duration::from_secs(delay.max(1))).await;
                attempts += 1;
                continue;
            }
            break response.error_for_status()?;
        };
        let page = response
            .json::<Page>()
            .await
            .context("Data API v2 page schema")?;
        validate_owner(&page, wallet)?;
        cursor = scan.accept(page)?;
        if cursor.is_none() {
            return Ok(scan.rows);
        }
    }
    bail!("Data API position scan exceeded page bound; refusing incomplete snapshot")
}

fn validate_owner(page: &Page, wallet: &str) -> Result<()> {
    for row in &page.data {
        if !row["proxy_wallet"]
            .as_str()
            .is_some_and(|owner| owner.eq_ignore_ascii_case(wallet))
        {
            bail!("Data API position owner missing or mismatched");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn page(rows: Value, cursor: Value) -> Page {
        serde_json::from_value(json!({"data": rows, "pagination": {"has_more": !cursor.is_null(), "next_cursor": cursor}})).unwrap()
    }
    #[test]
    fn cursor_scan_deduplicates_overlap_and_keeps_order() {
        let mut scan = Scan::default();
        scan.accept(page(json!([{"token_id":"11"}]), json!("a+/=")))
            .unwrap();
        scan.accept(page(
            json!([{"token_id":"11"},{"token_id":"12"}]),
            Value::Null,
        ))
        .unwrap();
        assert_eq!(
            scan.rows,
            vec![json!({"token_id":"11"}), json!({"token_id":"12"})]
        );
    }
    #[test]
    fn malformed_and_repeated_cursors_never_mean_empty_portfolio() {
        for value in [
            json!([]),
            json!({"data":[]}),
            json!({"data":[],"pagination":{}}),
            json!({"data":null,"pagination":{"next_cursor":null}}),
        ] {
            assert!(serde_json::from_value::<Page>(value).is_err());
        }
        let mut scan = Scan::default();
        scan.accept(page(json!([]), json!("same"))).unwrap();
        assert!(scan.accept(page(json!([]), json!("same"))).is_err());
        assert!(Scan::default().accept(page(json!([]), json!(""))).is_err());
    }
    #[test]
    fn conflicting_rows_and_scan_overflow_fail_closed() {
        let mut scan = Scan::default();
        scan.accept(page(
            json!([{"token_id":"11","current_size":1}]),
            json!("a"),
        ))
        .unwrap();
        assert!(scan
            .accept(page(
                json!([{"token_id":"11","current_size":2}]),
                Value::Null
            ))
            .is_err());
        let mut full = Scan::default();
        full.rows.resize(MAX_ROWS, Value::Null);
        assert!(full
            .accept(page(json!([{"token_id":"12"}]), Value::Null))
            .is_err());
        assert!(Scan::default().rows.is_empty()); // reconnect/wallet scans are isolated
    }

    #[test]
    fn owner_isolation_is_checked_before_snapshot_publication() {
        assert!(
            validate_owner(&page(json!([{"proxy_wallet":"0xAa"}]), Value::Null), "0xaa").is_ok()
        );
        assert!(
            validate_owner(&page(json!([{"proxy_wallet":"0xbb"}]), Value::Null), "0xaa").is_err()
        );
        assert!(validate_owner(&page(json!([{}]), Value::Null), "0xaa").is_err());
    }

    #[tokio::test]
    async fn http_scan_uses_v2_filters_and_preserves_opaque_cursor() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for index in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buf = [0; 1024];
                    let count = stream.read(&mut buf).unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&buf[..count]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap();
                let path = request.split_whitespace().nth(1).unwrap();
                let url = reqwest::Url::parse(&format!("http://localhost{path}")).unwrap();
                assert_eq!(url.path(), "/v2/positions");
                let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
                assert_eq!(query["user"], "0xaa");
                assert_eq!(query["status"], "OPEN");
                assert_eq!(query["include_archived"], "true");
                assert_eq!(query["filter_type"], "TOKENS");
                assert_eq!(query["filter_amount"], "0");
                assert!(!query.contains_key("offset") && !query.contains_key("sizeThreshold"));
                if index == 1 {
                    assert_eq!(query["cursor"], "a+/=");
                }
                let body = json!({"data":[{"token_id":format!("{}",index+1),"proxy_wallet":"0xAA"}],
                    "pagination":{"has_more":index==0,"next_cursor":if index==0 {json!("a+/=")} else {Value::Null}}}).to_string();
                write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
            }
        });
        let rows = fetch_rows(&reqwest::Client::new(), &base, "0xaa")
            .await
            .unwrap();
        server.join().unwrap();
        assert_eq!(rows.len(), 2);
    }
}
