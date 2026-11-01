use super::*;
use crate::exchange::{market_event_channel, PublicMarketReceiver};
use crate::types::MarketEvent;
use futures_util::{SinkExt, StreamExt};
use std::future::{pending, ready};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, Role};

#[derive(Default)]
struct TestSink {
    messages: Vec<Message>,
    fail: bool,
    stall: bool,
}

impl Sink<Message> for TestSink {
    type Error = &'static str;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.fail {
            Poll::Ready(Err("injected write failure"))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        self.messages.push(message);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.stall {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

fn scripted(
    events: Vec<(u64, Result<Message, Error>)>,
) -> impl Stream<Item = Result<Message, Error>> + Unpin {
    let start = Instant::now();
    Box::pin(
        futures_util::stream::iter(events)
            .then(move |(millis, message)| async move {
                tokio::time::sleep_until(start + Duration::from_millis(millis)).await;
                message
            })
            .chain(futures_util::stream::pending()),
    )
}

async fn drive(
    sink: &mut TestSink,
    events: Vec<(u64, Result<Message, Error>)>,
    settings: Settings,
) -> (SessionEnd, PublicMarketReceiver) {
    let (tx, rx) = market_event_channel(16);
    let end = run(
        sink,
        scripted(events),
        &["0xfeed".into()],
        &["btc/usd".into()],
        &tx,
        &AtomicBool::new(false),
        settings,
    )
    .await;
    (end, rx)
}

fn report(timestamp: u64) -> Message {
    let mut bytes = vec![0_u8; 160 + 9 * 32];
    bytes[120..128].copy_from_slice(&128_u64.to_be_bytes());
    bytes[152..160].copy_from_slice(&288_u64.to_be_bytes());
    bytes[368..384].copy_from_slice(&75_000_000_000_000_000_000_000_i128.to_be_bytes());
    Message::Text(
        serde_json::json!({"report": {
            "feedID": "0xfeed", "observationsTimestamp": timestamp,
            "fullReport": hex::encode(bytes)
        }})
        .to_string(),
    )
}

fn normal_close() -> Message {
    Message::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "rotation".into(),
    }))
}

#[tokio::test(start_paused = true)]
async fn regression_old_heartbeat_cancels_the_relative_read_timeout() {
    let start = Instant::now();
    let mut ping =
        tokio::time::interval_at(start + Duration::from_secs(30), Duration::from_secs(30));
    for _ in 0..5 {
        tokio::select! {
            biased;
            _ = ping.tick() => (),
            _ = tokio::time::timeout(Duration::from_secs(60), pending::<()>()) => panic!("old watchdog unexpectedly fired"),
        }
    }
    assert_eq!(start.elapsed(), Duration::from_secs(150));
}

#[tokio::test(start_paused = true)]
async fn outbound_ping_cannot_move_the_sixty_second_read_deadline() {
    // Isolate the read watchdog from the earlier production Pong deadline.
    let settings = Settings {
        pong_timeout: Duration::from_secs(90),
        ..Settings::default()
    };
    let mut sink = TestSink::default();
    let (end, _) = drive(&mut sink, vec![], settings).await;
    assert!(matches!(end.reason, EndReason::ReadTimeout));
    assert_eq!(
        end.ended_at - end.health.connected_at,
        Duration::from_secs(60)
    );
    assert_eq!(sink.messages, [Message::Ping(1_u64.to_be_bytes().to_vec())]);
}

#[tokio::test(start_paused = true)]
async fn missing_pong_times_out_even_while_valid_reports_arrive() {
    let mut sink = TestSink::default();
    let (end, rx) = drive(
        &mut sink,
        vec![
            (29_000, Ok(report(29))),
            (35_000, Ok(report(35))),
            (39_000, Ok(report(39))),
        ],
        Settings::default(),
    )
    .await;
    assert!(matches!(end.reason, EndReason::PongTimeout));
    assert_eq!(
        end.ended_at - end.health.connected_at,
        Duration::from_secs(40)
    );
    assert_eq!(end.health.reports, 3);
    assert_eq!(std::iter::from_fn(|| rx.try_recv().ok()).count(), 3);
    assert_eq!(end.health.last_report_source_ns, Some(39_000_000_000));
    assert_eq!(
        end.ended_at - end.health.last_report.unwrap(),
        Duration::from_secs(1)
    );
}

#[tokio::test(start_paused = true)]
async fn matched_pongs_measure_rtt_and_allow_multiple_heartbeat_cycles() {
    let mut sink = TestSink::default();
    let (end, _) = drive(
        &mut sink,
        vec![
            (30_025, Ok(Message::Pong(1_u64.to_be_bytes().to_vec()))),
            (60_040, Ok(Message::Pong(2_u64.to_be_bytes().to_vec()))),
            (90_050, Ok(Message::Pong(3_u64.to_be_bytes().to_vec()))),
            (95_000, Ok(normal_close())),
        ],
        Settings::default(),
    )
    .await;
    assert_eq!(end.reason.close_code(), Some(1000));
    assert_eq!(end.health.matched_pongs, 3);
    assert_eq!(end.health.last_rtt, Some(Duration::from_millis(50)));
    assert!(end.health.pending_ping.is_none());
    assert!(end.health.last_report.is_none()); // transport liveness != usable data
    assert_eq!(sink.messages.len(), 3);
}

#[tokio::test(start_paused = true)]
async fn unsolicited_duplicate_and_wrong_pongs_do_not_acknowledge_a_new_ping() {
    let mut sink = TestSink::default();
    let (end, _) = drive(
        &mut sink,
        vec![
            (10_000, Ok(Message::Pong(vec![]))),
            (30_005, Ok(Message::Pong(1_u64.to_be_bytes().to_vec()))),
            (60_005, Ok(Message::Pong(1_u64.to_be_bytes().to_vec()))),
            (65_000, Ok(Message::Pong(999_u64.to_be_bytes().to_vec()))),
            (69_000, Ok(Message::Ping(b"server-ping".to_vec()))),
        ],
        Settings::default(),
    )
    .await;
    assert!(matches!(end.reason, EndReason::PongTimeout));
    assert_eq!(
        end.ended_at - end.health.connected_at,
        Duration::from_secs(70)
    );
    assert_eq!(end.health.matched_pongs, 1);
    assert_eq!(end.health.unmatched_pongs, 3);
    assert_eq!(
        sink.messages.last(),
        Some(&Message::Pong(b"server-ping".to_vec()))
    );
}

#[tokio::test(start_paused = true)]
async fn actual_inbound_frames_renew_read_silence_but_not_report_freshness() {
    let settings = Settings {
        ping_interval: Duration::from_secs(300),
        ..Settings::default()
    };
    let (end, _) = drive(
        &mut TestSink::default(),
        vec![
            (25_000, Ok(Message::Text("bad json".into()))),
            (50_000, Ok(Message::Ping(vec![7]))),
            (75_000, Ok(Message::Pong(vec![8]))),
        ],
        settings,
    )
    .await;
    assert!(matches!(end.reason, EndReason::ReadTimeout));
    assert_eq!(
        end.ended_at - end.health.connected_at,
        Duration::from_secs(135)
    );
    assert_eq!(end.health.frames, 3);
    assert_eq!(end.health.unparsed_data_frames, 1);
    assert!(end.health.last_report.is_none());
}

#[tokio::test(start_paused = true)]
async fn expired_deadlines_win_simultaneously_ready_frames_and_pings() {
    let mut h = Health::new(Settings::default());
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(matches!(
        h.next(ready(()), &mut ping).await,
        Action::End(EndReason::ReadTimeout)
    ));
    h.received();
    h.begin_ping();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(matches!(
        h.next(ready(()), &mut ping).await,
        Action::End(EndReason::PongTimeout)
    ));
}

#[tokio::test(start_paused = true)]
async fn send_failures_are_not_silently_ignored() {
    for (events, expected) in [
        (vec![], "ping_send_failed"),
        (vec![(1000, Ok(Message::Ping(vec![1])))], "pong_send_failed"),
    ] {
        let mut sink = TestSink {
            fail: true,
            ..TestSink::default()
        };
        let (end, _) = drive(&mut sink, events, Settings::default()).await;
        assert_eq!(end.reason.label(), expected);
        assert_eq!(end.reason.write_error(), Some("injected write failure"));
    }
}

#[tokio::test(start_paused = true)]
async fn blocked_control_writes_cannot_outlive_read_or_pong_deadlines() {
    let mut h = Health::new(Settings::default());
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(matches!(
        h.control_write(pending::<()>()).await,
        Err(EndReason::ReadTimeout)
    ));
    assert_eq!(h.connected_at.elapsed(), Duration::from_secs(60));
    h.received();
    h.begin_ping();
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(matches!(
        h.control_write(pending::<()>()).await,
        Err(EndReason::PongTimeout)
    ));
    assert_eq!(h.connected_at.elapsed(), Duration::from_secs(70));

    let mut sink = TestSink {
        stall: true,
        ..TestSink::default()
    };
    let (end, _) = drive(&mut sink, vec![], Settings::default()).await;
    assert!(matches!(end.reason, EndReason::PongTimeout));
    assert_eq!(
        end.ended_at - end.health.connected_at,
        Duration::from_secs(40)
    );
}

#[tokio::test(start_paused = true)]
async fn close_io_protocol_and_eof_have_distinct_diagnostics() {
    use tokio_tungstenite::tungstenite::error::ProtocolError;
    for (message, expected) in [
        (Ok(normal_close()), "server_close"),
        (
            Err(Error::Io(std::io::Error::from(
                std::io::ErrorKind::ConnectionReset,
            ))),
            "read_io_error",
        ),
        (
            Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)),
            "read_protocol_error",
        ),
    ] {
        let (end, _) = drive(
            &mut TestSink::default(),
            vec![(1000, message)],
            Settings::default(),
        )
        .await;
        assert_eq!(end.reason.label(), expected);
        // The diagnostic path takes only session data, never credentials.
        end.log(uuid::Uuid::nil(), 1, Some(Duration::from_millis(200)));
    }
    let (tx, _rx) = market_event_channel(1);
    let end = run(
        TestSink::default(),
        futures_util::stream::empty(),
        &[],
        &[],
        &tx,
        &AtomicBool::new(false),
        Settings::default(),
    )
    .await;
    assert_eq!(end.reason.label(), "stream_eof");
}

#[tokio::test(start_paused = true)]
async fn reconnect_and_concurrent_instances_have_fresh_private_watchdogs() {
    let mut sink1 = TestSink::default();
    let mut sink2 = TestSink::default();
    let (first, second) = tokio::join!(
        drive(&mut sink1, vec![], Settings::default()),
        drive(
            &mut sink2,
            vec![
                (30_100, Ok(Message::Pong(1_u64.to_be_bytes().to_vec()))),
                (45_000, Ok(normal_close()))
            ],
            Settings::default()
        )
    );
    assert!(matches!(first.0.reason, EndReason::PongTimeout));
    assert_eq!(second.0.reason.close_code(), Some(1000));
    assert_eq!(second.0.health.matched_pongs, 1);
    let (reconnected, _) = drive(
        &mut TestSink::default(),
        vec![
            (1000, Ok(report(1))),
            (30_100, Ok(Message::Pong(1_u64.to_be_bytes().to_vec()))),
            (45_000, Ok(normal_close())),
        ],
        Settings::default(),
    )
    .await;
    assert_eq!(reconnected.health.pings, 1);
    assert_eq!(reconnected.health.reports, 1);
    assert_eq!(reconnected.health.matched_pongs, 1);
    assert_eq!(
        reconnected.ended_at - reconnected.health.connected_at,
        Duration::from_secs(45)
    );
}

#[tokio::test(start_paused = true)]
async fn report_order_replay_and_bounded_latest_value_overflow_are_unchanged() {
    // Adapter preserves arrival order and duplicates; downstream source-time
    // policy remains the authority. Overflow still keeps the newest values.
    let (tx, rx) = market_event_channel(2);
    let end = run(
        TestSink::default(),
        scripted(vec![
            (1, Ok(report(1))),
            (2, Ok(report(2))),
            (3, Ok(report(2))),
            (4, Ok(report(3))),
            (5, Ok(normal_close())),
        ]),
        &["0xfeed".into()],
        &["btc/usd".into()],
        &tx,
        &AtomicBool::new(false),
        Settings::default(),
    )
    .await;
    assert_eq!(end.health.reports, 4);
    for timestamp in [2, 3] {
        let MarketEvent::SpotPrice(price) = rx.try_recv().unwrap() else {
            panic!("expected report");
        };
        assert_eq!(price.timestamp_ns, timestamp * 1_000_000_000);
        assert_eq!(price.price, 75_000.0);
        assert_eq!(price.symbol, "btc/usd");
    }
    assert!(rx.try_recv().is_err());
    let (_, rx) = drive(
        &mut TestSink::default(),
        vec![
            (1, Ok(report(1))),
            (2, Ok(report(2))),
            (3, Ok(report(2))),
            (4, Ok(normal_close())),
        ],
        Settings::default(),
    )
    .await;
    for timestamp in [1, 2, 2] {
        let MarketEvent::SpotPrice(price) = rx.try_recv().unwrap() else {
            panic!("expected report");
        };
        assert_eq!(price.timestamp_ns, timestamp * 1_000_000_000);
    }
}

#[tokio::test(start_paused = true)]
async fn consumer_closed_and_shutdown_stop_without_reconnecting() {
    let (tx, rx) = market_event_channel(1);
    drop(rx);
    let end = run(
        TestSink::default(),
        scripted(vec![(1, Ok(report(1)))]),
        &["0xfeed".into()],
        &["btc/usd".into()],
        &tx,
        &AtomicBool::new(false),
        Settings::default(),
    )
    .await;
    assert!(matches!(end.reason, EndReason::ConsumerClosed));
    assert!(end.should_stop());
    let end = run(
        TestSink::default(),
        futures_util::stream::pending(),
        &[],
        &[],
        &tx,
        &AtomicBool::new(true),
        Settings::default(),
    )
    .await;
    assert!(matches!(end.reason, EndReason::Shutdown));
    assert!(end.should_stop());
}

#[tokio::test(start_paused = true)]
async fn real_websocket_echoes_ping_and_streams_reports_until_server_close() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let client =
        tokio_tungstenite::WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let mut server =
        tokio_tungstenite::WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let (write, read) = client.split();
    let (tx, rx) = market_event_channel(8);
    let shutdown = AtomicBool::new(false);
    let feeds = ["0xfeed".into()];
    let symbols = ["btc/usd".into()];
    let (end, ()) = tokio::join!(
        run(
            write,
            read,
            &feeds,
            &symbols,
            &tx,
            &shutdown,
            Settings::default()
        ),
        async {
            server.send(report(1)).await.unwrap();
            for expected in 1_u64..=3 {
                let Message::Ping(payload) = server.next().await.unwrap().unwrap() else {
                    panic!("expected ping");
                };
                assert_eq!(payload, expected.to_be_bytes());
                // Tungstenite queues the RFC Pong automatically when reading Ping.
                server.flush().await.unwrap();
                server.send(report(expected + 1)).await.unwrap();
            }
            server.send(normal_close()).await.unwrap();
        }
    );
    assert_eq!(end.reason.close_code(), Some(1000));
    assert_eq!(end.health.matched_pongs, 3);
    assert_eq!(end.health.reports, 4);
    assert_eq!(std::iter::from_fn(|| rx.try_recv().ok()).count(), 4);
}

#[tokio::test]
async fn local_server_reconnect_resubscribes_and_delivers_to_the_same_receiver() {
    // Entire production reconnect loop, with dummy authentication on localhost.
    // Real time avoids paused-clock auto-advance racing OS socket readiness.
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (tx, rx) = market_event_channel(8);
        let shutdown = std::sync::Arc::new(AtomicBool::new(false));
        let tls = crate::exchange::ws_tls::WsTlsConfig::load_native().unwrap();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel::<()>();
        let client = super::super::chainlink_stream_ws_task(
            url,
            vec!["0xfeed".into(), "0xtwap".into()],
            vec!["btc/usd".into(), "btc/usd/twap".into()],
            "local-test-key".into(),
            "local-test-secret".into(),
            tx,
            shutdown.clone(),
            tls,
        );
        let server = async {
            let mut finish_rx = Some(finish_rx);
            for session in 1..=2 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_hdr_async(
                    tcp,
                    |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     response| {
                        assert_eq!(
                            request.uri().path_and_query().unwrap().as_str(),
                            "/api/v1/ws?feedIDs=0xfeed,0xtwap"
                        );
                        assert_eq!(request.headers()["Authorization"], "local-test-key");
                        assert!(request
                            .headers()
                            .contains_key("X-Authorization-Signature-SHA256"));
                        Ok(response)
                    },
                )
                .await
                .unwrap();
                ws.send(report(session)).await.unwrap();
                if session == 2 {
                    finish_rx.take().unwrap().await.unwrap();
                }
                ws.send(normal_close()).await.unwrap();
            }
        };
        let consumer = async {
            for timestamp in [1, 2] {
                let event = loop {
                    if let Ok(event) = rx.try_recv() {
                        break event;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                };
                let MarketEvent::SpotPrice(price) = event else {
                    panic!("expected report");
                };
                assert_eq!(price.timestamp_ns, timestamp * 1_000_000_000);
            }
            shutdown.store(true, Ordering::Relaxed);
            finish_tx.send(()).unwrap();
        };
        tokio::join!(client, server, consumer);
    })
    .await
    .expect("local reconnect loop timed out");
}

#[test]
#[ignore = "focused timer/select CPU benchmark; no network or queues"]
fn ready_read_watchdog_before_after_benchmark() {
    const N: usize = 100_000;
    const WARMUP: usize = 5000;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut h = Health::new(Settings::default());
        let mut ping = tokio::time::interval_at(Instant::now() + Duration::from_secs(30), Duration::from_secs(30));
        let mut old_ping = tokio::time::interval_at(Instant::now() + Duration::from_secs(30), Duration::from_secs(30));
        let mut old = Vec::with_capacity(N);
        let mut new = Vec::with_capacity(N);
        for index in 0..N + WARMUP {
            for is_new in if index % 2 == 0 { [false, true] } else { [true, false] } {
                let start = std::time::Instant::now();
                if is_new {
                    assert!(matches!(h.next(ready(std::hint::black_box(())), &mut ping).await, Action::Inbound(())));
                    h.received();
                } else {
                    tokio::select! {
                        biased;
                        _ = old_ping.tick() => panic!("benchmark exceeded ping interval"),
                        result = tokio::time::timeout(Duration::from_secs(60), ready(std::hint::black_box(()))) => result.unwrap(),
                    }
                }
                let elapsed = start.elapsed().as_nanos() as u64;
                if index >= WARMUP { if is_new { new.push(elapsed); } else { old.push(elapsed); } }
            }
        }
        for (version, mut samples) in [("old", old), ("absolute_read_and_pong", new)] {
            samples.sort_unstable();
            let at = |q: f64| samples[((samples.len() - 1) as f64 * q).ceil() as usize];
            println!("CHAINLINK_WATCHDOG_BENCH {{\"version\":\"{version}\",\"n\":{N},\"warmup\":{WARMUP},\"unit\":\"ns\",\"p50\":{},\"p99\":{},\"p999\":{},\"max\":{},\"queue_depth\":null,\"overflow\":null}}", at(0.5), at(0.99), at(0.999), samples.last().unwrap());
        }
    });
}
