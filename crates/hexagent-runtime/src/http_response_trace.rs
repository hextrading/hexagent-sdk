//! Allowlisted, bounded response identifiers. No auth/cookie/request headers.
//! Copied on 403 only; formatting belongs to the background audit writer.
#[derive(Clone, Copy, Debug)]
pub struct TraceId {
    bytes: [u8; 64],
    len: u8,
    pub truncated: bool,
}

impl Default for TraceId {
    fn default() -> Self {
        Self {
            bytes: [0; 64],
            len: 0,
            truncated: false,
        }
    }
}

impl TraceId {
    fn from_header(value: Option<&reqwest::header::HeaderValue>) -> Self {
        let mut id = Self::default();
        if let Some(value) = value {
            let bytes = value.as_bytes();
            // Reject control/non-ASCII bytes rather than emitting untrusted log text.
            if !bytes.iter().all(|b| b.is_ascii_graphic()) {
                return id;
            }
            let len = bytes.len().min(id.bytes.len());
            id.bytes[..len].copy_from_slice(&bytes[..len]);
            id.len = len as u8;
            id.truncated = bytes.len() > len;
        }
        id
    }
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or("")
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HttpResponseTrace {
    pub status: u16,
    pub cf_ray: TraceId,
    pub request_id: TraceId,
}

impl HttpResponseTrace {
    pub fn capture(status: u16, headers: &reqwest::header::HeaderMap) -> Self {
        if status != 403 {
            return Self::default();
        }
        Self {
            status,
            cf_ray: TraceId::from_header(headers.get("cf-ray")),
            request_id: TraceId::from_header(
                headers
                    .get("x-request-id")
                    .or_else(|| headers.get("x-amzn-requestid")),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_forbidden_response_identifiers_are_copied_and_bounded() {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert("cf-ray", "a4b87e109a4cef13-LHR".parse().unwrap());
        h.insert("x-request-id", "x".repeat(100).parse().unwrap());
        h.insert("set-cookie", "secret-cookie".parse().unwrap());
        let t = HttpResponseTrace::capture(403, &h);
        assert_eq!(t.cf_ray.as_str(), "a4b87e109a4cef13-LHR");
        assert_eq!(t.request_id.as_str().len(), 64);
        assert!(t.request_id.truncated);
        assert_eq!(HttpResponseTrace::capture(200, &h).status, 0);
        assert!(!format!("{t:?}").contains("secret-cookie"));
        h.insert(
            "cf-ray",
            reqwest::header::HeaderValue::from_bytes(b"bad\tvalue").unwrap(),
        );
        assert_eq!(HttpResponseTrace::capture(403, &h).cf_ray.as_str(), "");
    }
}
