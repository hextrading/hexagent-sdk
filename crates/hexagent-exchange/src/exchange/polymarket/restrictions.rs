//! Explicit venue restrictions, carried in existing OrderUpdate error messages.
//! Parsing is allocation-free. The strategy owner owns its retry deadline;
//! execution workers never sleep/retry an order or mutate strategy state.

fn contains(text: &str, needle: &str) -> bool {
    text.as_bytes()
        .windows(needle.len())
        .any(|part| part.eq_ignore_ascii_case(needle.as_bytes()))
}

pub fn cooldown_secs(text: &str) -> Option<u64> {
    let default = if contains(text, "post_only_mode") || contains(text, "post-only mode:") {
        120
    } else if contains(text, "venue_restriction=restarting") || contains(text, "status 425 (") {
        2
    } else {
        return None;
    };
    let mut retry: Option<u64> = None;
    for marker in ["retry_after_seconds=", "\"retry_after_seconds\":"] {
        if let Some(index) = text.find(marker) {
            let value = text[index + marker.len()..].trim_start();
            let end = value.bytes().take_while(u8::is_ascii_digit).count();
            if let Ok(seconds) = value[..end].parse::<u64>() {
                retry = Some(retry.unwrap_or(1).max(seconds.max(1)));
            }
        }
    }
    Some(retry.unwrap_or(default))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_explicit_modes_gate_trading_and_server_delay_wins() {
        assert_eq!(
            cooldown_secs("status 425 (busy) retry_after_seconds=17"),
            Some(17)
        );
        assert_eq!(
            cooldown_secs("status 503 ({\"code\":\"post_only_mode\",\"retry_after_seconds\":79})"),
            Some(79)
        );
        assert_eq!(
            cooldown_secs("post-only mode: only post-only orders and cancels are allowed"),
            Some(120)
        );
        assert_eq!(cooldown_secs("post-only crosses book"), None);
        assert_eq!(cooldown_secs("status 503 (upstream unavailable)"), None);
        assert_eq!(cooldown_secs("timeout"), None);
        assert_eq!(
            cooldown_secs("post_only_mode {\"retry_after_seconds\":300} retry_after_seconds=60"),
            Some(300)
        );
    }
}
