//! Rejected factories must never compact numeric strategy ownership.
use super::*;
use hexagent_strategy::factory::StrategyFactory;

struct FixtureStrategy(String);
impl Strategy for FixtureStrategy {
    fn name(&self) -> &str {
        "fixture"
    }
    fn instance_id(&self) -> &str {
        &self.0
    }
}

struct FixtureFactory {
    calls: Arc<std::sync::Mutex<Vec<(String, usize)>>>,
}
impl StrategyFactory for FixtureFactory {
    fn name(&self) -> &'static str {
        "fixture"
    }
    fn build(&self, deps: StrategyBuildDeps<'_>) -> Option<Box<dyn Strategy>> {
        self.calls
            .lock()
            .unwrap()
            .push((deps.cfg.instance_id.clone(), deps.strategy_index));
        if deps
            .cfg
            .params
            .get("reject")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            None
        } else {
            Some(Box::new(FixtureStrategy(deps.cfg.instance_id.clone())))
        }
    }
}

fn fixture(
    rejected: Option<usize>,
    disabled: bool,
) -> (Engine, Arc<std::sync::Mutex<Vec<(String, usize)>>>) {
    let mut text = "[general]\nmode='live'\n".to_string();
    if disabled {
        text.push_str(
            "[[strategies]]\nname='missing-factory'\nenabled=false\ninstance_id='disabled'\n",
        );
    }
    for index in 0..3 {
        text.push_str(&format!("[[strategies]]\nname='fixture'\nenabled=true\ninstance_id='btc0{}'\n[strategies.params]\nreject={}\n", index+1, rejected == Some(index)));
    }
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut registry = StrategyRegistry::new();
    registry.register(FixtureFactory {
        calls: calls.clone(),
    });
    (Engine::new(toml::from_str(&text).unwrap(), registry), calls)
}

#[test]
fn rejected_enabled_factory_aborts_before_later_owner_can_shift() {
    for index in 0..3 {
        let (engine, calls) = fixture(Some(index), true);
        let result = engine.build_strategies(HashMap::new(), HashMap::new(), &HashMap::new());
        let error = match result {
            Err(error) => error.to_string(),
            Ok(_) => panic!("rejected owner was silently skipped"),
        };
        assert!(
            error.contains(&format!("instance `btc0{}`", index + 1)),
            "{error}"
        );
        assert!(error.contains(&format!("owner {index}")), "{error}");
        assert_eq!(calls.lock().unwrap().len(), index + 1);
    }
}

#[test]
fn accepted_owners_preserve_enabled_order_across_repeated_startup() {
    let (engine, calls) = fixture(None, true);
    for _ in 0..2 {
        let strategies = engine
            .build_strategies(HashMap::new(), HashMap::new(), &HashMap::new())
            .unwrap();
        assert_eq!(
            strategies
                .iter()
                .map(|s| s.instance_id())
                .collect::<Vec<_>>(),
            ["btc01", "btc02", "btc03"]
        );
    }
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            ("btc01".into(), 0),
            ("btc02".into(), 1),
            ("btc03".into(), 2),
            ("btc01".into(), 0),
            ("btc02".into(), 1),
            ("btc03".into(), 2)
        ]
    );
}

#[test]
fn unknown_enabled_factory_is_a_startup_error() {
    let config = toml::from_str("[general]\nmode='live'\n[[strategies]]\nname='missing'\nenabled=true\ninstance_id='owner'\n").unwrap();
    let engine = Engine::new(config, StrategyRegistry::new());
    assert!(engine
        .build_strategies(HashMap::new(), HashMap::new(), &HashMap::new())
        .is_err());
}

#[test]
fn rejected_live_startup_stops_producers_and_finishes_shutdown() {
    let (mut engine, _) = fixture(Some(0), false);
    let root = std::env::temp_dir().join(format!(
        "hexagent-rejected-startup-{}-{}",
        std::process::id(),
        now_ns()
    ));
    engine.config.recording.output_dir = root.to_string_lossy().into_owned();
    engine.config.general.latency_record_enabled = false;
    let shutdown = ShutdownToken::new();
    let error = engine.run_live(shutdown.clone()).unwrap_err();
    assert!(error.to_string().contains("failed construction"));
    assert!(shutdown.is_finished());
    if root.exists() {
        std::fs::remove_dir_all(root).unwrap();
    }
}
