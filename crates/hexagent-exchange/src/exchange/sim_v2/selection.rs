//! Optional offline-calibrated candidate selection. Owned by the simulator
//! thread, never installed in a live exchange. No price or time rewriting.
//! All candidates have already passed the existing matching constraints.
use anyhow::{ensure, Result};
use arrayvec::ArrayString;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub const N: usize = 9;
const CAPACITY: usize = 16_384;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RoleModel {
    /// Ridge prediction of signed future mid change in ticks, from causal X.
    pub markout: [f64; N],
    /// Logistic retention coefficients: causal X followed by future-mid change.
    pub retention: [f64; N],
    pub markout_weight: f64,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelModel {
    pub fill: RoleModel,
    /// Conditional filled/candidate quantity in (0,1), logistic link.
    /// None preserves full candidate quantity (e.g. unidentified taker sizes).
    pub quantity: Option<[f64; N]>,
    #[serde(default)]
    pub quantity_markout_weight: f64,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionModel {
    pub schema_version: u32,
    pub horizon_ms: u64,
    pub max_label_gap_ms: u64,
    pub training_end_ns: u64,
    pub maker: RoleModel,
    pub taker: RoleModel,
    #[serde(default)]
    pub maker_trade: Option<ChannelModel>,
    #[serde(default)]
    pub maker_book: Option<ChannelModel>,
    #[serde(default)]
    pub taker_sweep: Option<ChannelModel>,
}
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Mode {
    Off,
    Collect,
    Causal,
    Forward,
}
#[derive(Serialize)]
pub struct SelectionAudit {
    pub coid: ArrayString<128>,
    pub iid: ArrayString<64>,
    pub token: ArrayString<128>,
    pub now_ns: u64,
    pub role: &'static str,
    pub channel: &'static str,
    pub x: [f64; N],
    pub mid: f64,
    pub tick: f64,
    pub signed_side: f64,
    pub candidate_qty: f64,
    pub kept_qty: f64,
    pub forward_mid: Option<f64>,
    pub predicted_markout_ticks: f64,
    pub probability: f64,
    pub uniform: f64,
    pub strength: f64,
    pub quantity_fraction: f64,
    pub channel_model: bool,
}
#[derive(Default, Serialize)]
pub struct SelectionStats {
    pub candidates: u64,
    pub suppressed_candidates: u64,
    pub candidate_qty: f64,
    pub suppressed_qty: f64,
    pub partial_candidates: u64,
    pub missing_forward_candidates: u64,
    pub forward_lookups: u64,
    pub audit_emitted: u64,
    pub audit_drained: u64,
    pub audit_high_water: usize,
    pub audit_overflows: u64,
}
pub struct Selection {
    pub mode: Mode,
    pub model: Option<SelectionModel>,
    strength: f64,
    role_strengths: [Option<f64>; 2],
    seed: u64,
    pub stats: SelectionStats,
    audit: bool,
    rows: VecDeque<SelectionAudit>,
    forward: Option<(ArrayString<128>, u64, f64)>,
}
impl Default for Selection {
    fn default() -> Self {
        Self {
            mode: Mode::Off,
            model: None,
            strength: 0.,
            role_strengths: [None, None],
            seed: 0,
            stats: SelectionStats::default(),
            audit: false,
            rows: VecDeque::new(),
            forward: None,
        }
    }
}

pub fn features(
    side: f64,
    price: f64,
    mid: f64,
    spread: f64,
    tick: f64,
    ahead: f64,
    age_ns: u64,
    candidate: f64,
    request: f64,
    entry: f64,
) -> [f64; N] {
    let tick = tick.max(1e-6);
    [
        1.,
        (side * (mid - price) / tick).clamp(-20., 20.) / 10.,
        (spread / tick).clamp(0., 20.) / 10.,
        (ahead.max(0.) / request.max(1e-6)).ln_1p().min(10.) / 5.,
        (age_ns as f64 / 1e9).ln_1p().min(10.) / 5.,
        (candidate / request.max(1e-6)).clamp(0., 1.),
        price.clamp(0., 1.),
        side,
        if entry > 0. {
            (side * (mid - entry) / tick).clamp(-20., 20.) / 10.
        } else {
            0.
        },
    ]
}
fn dot(a: &[f64; N], b: &[f64; N]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}
fn uniform(iid: &str, coid: &str, role: &str, seed: u64) -> f64 {
    let mut h = 0xcbf29ce484222325u64 ^ seed;
    for part in [iid, coid, role] {
        for b in part.bytes().chain([0xff]) {
            h = (h ^ b as u64).wrapping_mul(0x100000001b3);
        }
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^= h >> 31;
    (h >> 11) as f64 / (1u64 << 53) as f64
}
impl Selection {
    pub fn configure(
        &mut self,
        mode: &str,
        path: &str,
        strength: f64,
        seed: u64,
        audit: bool,
    ) -> Result<()> {
        let mode = match mode {
            "off" | "" => Mode::Off,
            "collect" => Mode::Collect,
            "causal" => Mode::Causal,
            "forward" => Mode::Forward,
            _ => anyhow::bail!("unknown selection mode"),
        };
        ensure!(
            strength.is_finite() && (0.0..=1.0).contains(&strength),
            "invalid selection strength"
        );
        let model = if path.is_empty() {
            None
        } else {
            let m: SelectionModel = serde_json::from_reader(std::fs::File::open(path)?)?;
            ensure!(
                (m.schema_version == 1 || m.schema_version == 2)
                    && m.horizon_ms > 0
                    && m.horizon_ms <= 5000
                    && m.max_label_gap_ms <= 1000,
                "invalid selection schema/horizon"
            );
            for r in [&m.maker, &m.taker] {
                ensure!(
                    r.markout
                        .iter()
                        .chain(r.retention.iter())
                        .all(|v| v.is_finite())
                        && r.markout_weight.is_finite(),
                    "nonfinite selection model"
                );
            }
            let channels = [&m.maker_trade, &m.maker_book, &m.taker_sweep];
            ensure!(m.schema_version == 2 || channels.iter().all(|c| c.is_none()), "channel models require schema 2");
            for c in channels.into_iter().flatten() {
                ensure!(c.fill.markout.iter().chain(c.fill.retention.iter()).all(|v| v.is_finite())
                    && c.fill.markout_weight.is_finite() && c.quantity_markout_weight.is_finite()
                    && c.quantity.as_ref().is_none_or(|q| q.iter().all(|v| v.is_finite())), "nonfinite channel model");
            }
            Some(m)
        };
        ensure!(
            !matches!(mode, Mode::Causal | Mode::Forward) || model.is_some(),
            "selection requires a model"
        );
        self.mode = mode;
        self.model = model;
        self.strength = strength;
        self.role_strengths = [None, None];
        self.seed = seed;
        self.audit = audit;
        self.rows = if audit {
            VecDeque::with_capacity(CAPACITY)
        } else {
            VecDeque::new()
        };
        Ok(())
    }
    /// Startup-only overrides. Validate both before changing either role.
    pub fn configure_role_strengths(&mut self, maker: Option<f64>, taker: Option<f64>) -> Result<()> {
        ensure!([maker, taker].into_iter().flatten().all(|v| v.is_finite() && (0.0..=1.0).contains(&v)), "invalid role selection strength");
        self.role_strengths = [maker, taker];
        Ok(())
    }
    pub fn needs_forward(&self) -> bool {
        matches!(self.mode, Mode::Collect | Mode::Forward)
    }
    pub fn horizon_ns(&self) -> u64 {
        self.model.as_ref().map_or(1000, |m| m.horizon_ms) * 1_000_000
    }
    pub fn gap_ns(&self) -> u64 {
        self.model.as_ref().map_or(250, |m| m.max_label_gap_ms) * 1_000_000
    }
    pub fn set_forward(&mut self, token: &str, now: u64, mid: Option<f64>) {
        self.stats.forward_lookups += 1;
        self.forward = mid
            .filter(|v| v.is_finite() && *v > 0. && *v < 1.)
            .map(|v| {
                (
                    ArrayString::from(token).expect("selection token capacity"),
                    now,
                    v,
                )
            });
    }
    /// Stable owner/order/role random rank: repeated candidates do not obtain
    /// fresh independent lottery draws. State/price changes may change P.
    pub fn select(
        &mut self,
        iid: &str,
        coid: &str,
        token: &str,
        now: u64,
        role: &'static str,
        channel: &'static str,
        x: [f64; N],
        mid: f64,
        tick: f64,
        side: f64,
        qty: f64,
    ) -> f64 {
        if self.mode == Mode::Off || qty <= 0. {
            return qty;
        }
        assert!(x.iter().all(|v| v.is_finite()) && qty.is_finite());
        let fwd = if self.needs_forward() {
            self.forward
                .as_ref()
                .filter(|(s, t, _)| s.as_str() == token && *t == now)
                .map(|(_, _, m)| *m)
        } else {
            None
        };
        let strength = self.role_strengths[usize::from(role != "maker")].unwrap_or(self.strength);
        let mut prediction = 0.;
        let mut quantity_fraction = 1.;
        let mut channel_model = false;
        let probability = if matches!(self.mode, Mode::Causal | Mode::Forward) {
            let model = self.model.as_ref().unwrap();
            let channel_fit = match (role, channel) {
                ("maker", "trade") => model.maker_trade.as_ref(),
                ("maker", "book") => model.maker_book.as_ref(),
                ("taker", "sweep") => model.taker_sweep.as_ref(),
                _ => None,
            };
            channel_model = channel_fit.is_some();
            let m = if let Some(c) = channel_fit { &c.fill } else if role == "maker" {
                &model.maker
            } else {
                &model.taker
            };
            prediction = dot(&m.markout, &x).clamp(-20., 20.);
            // Missing future labels use the causal prediction, with an explicit counter.
            let markout = if self.mode == Mode::Forward {
                fwd.map(|f| (side * (f - mid) / tick.max(1e-6)).clamp(-20., 20.))
                    .unwrap_or(prediction)
            } else {
                prediction
            };
            if let Some(c) = channel_fit {
                if let Some(q) = &c.quantity {
                    let q = 1. / (1. + (-(dot(q, &x) + c.quantity_markout_weight * markout)).clamp(-40., 40.).exp());
                    quantity_fraction = (1. - strength * (1. - q)).clamp(0., 1.);
                }
            }
            let p = 1.
                / (1.
                    + (-(dot(&m.retention, &x) + m.markout_weight * markout))
                        .clamp(-40., 40.)
                        .exp());
            (1. - strength * (1. - p)).clamp(0., 1.)
        } else {
            1.
        };
        let u = uniform(iid, coid, role, self.seed);
        let kept = if u < probability { qty * quantity_fraction } else { 0. };
        self.stats.partial_candidates += (kept > 0. && kept < qty) as u64;
        self.stats.candidates += 1;
        self.stats.candidate_qty += qty;
        self.stats.suppressed_candidates += (kept == 0.) as u64;
        self.stats.suppressed_qty += qty - kept;
        self.stats.missing_forward_candidates += (self.needs_forward() && fwd.is_none()) as u64;
        if self.audit {
            if self.rows.len() == CAPACITY {
                self.stats.audit_overflows += 1;
                panic!("selection audit overflow; no silent evidence loss");
            }
            self.rows.push_back(SelectionAudit {
                coid: ArrayString::from(coid).expect("coid capacity"),
                iid: ArrayString::from(iid).expect("owner capacity"),
                token: ArrayString::from(token).expect("token capacity"),
                now_ns: now,
                role,
                channel,
                x,
                mid,
                tick,
                signed_side: side,
                candidate_qty: qty,
                kept_qty: kept,
                forward_mid: fwd,
                predicted_markout_ticks: prediction,
                probability,
                uniform: u,
                strength,
                quantity_fraction,
                channel_model,
            });
            self.stats.audit_emitted += 1;
            self.stats.audit_high_water = self.stats.audit_high_water.max(self.rows.len());
        }
        kept
    }
    pub fn drain(&mut self) -> impl Iterator<Item = SelectionAudit> + '_ {
        self.stats.audit_drained += self.rows.len() as u64;
        self.rows.drain(..)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn m() -> SelectionModel {
        SelectionModel {
            schema_version: 1,
            horizon_ms: 1000,
            max_label_gap_ms: 250,
            training_end_ns: 1,
            maker_trade: None, maker_book: None, taker_sweep: None,
            maker: RoleModel {
                markout: [0.; N],
                retention: [0.; N],
                markout_weight: 1.,
            },
            taker: RoleModel {
                markout: [0.; N],
                retention: [0.; N],
                markout_weight: 1.,
            },
        }
    }
    #[test]
    fn channel_quantity_is_bounded_optional_and_zero_strength_is_identity() {
        let mut s = Selection::default();
        s.configure("collect", "", 1., 42, true).unwrap();
        let mut model = m(); model.schema_version = 2;
        let mut fill = model.maker.clone(); fill.retention[0]=40.; fill.markout_weight=0.;
        model.maker_book=Some(ChannelModel { fill, quantity:Some([0.;N]), quantity_markout_weight:0. });
        s.model=Some(model);s.mode=Mode::Forward;
        let mut x=[0.;N];x[0]=1.;
        let q=s.select("owner","one","up",1,"maker","book",x,0.5,0.01,1.,14.);
        assert_eq!(q,7.);
        assert_eq!(s.select("owner","one","up",1,"maker","book",x,0.5,0.01,1.,14.),q);
        assert!(s.drain().all(|r| r.channel_model && r.quantity_fraction==0.5));
        s.configure_role_strengths(Some(0.),Some(0.)).unwrap();
        assert_eq!(s.select("owner","one","up",1,"maker","book",x,0.5,0.01,1.,14.),14.);
        assert_eq!(s.select("owner","one","up",1,"taker","sweep",x,0.5,0.01,1.,14.),14.);
        s.mode=Mode::Collect;
        assert_eq!(s.select("owner","one","up",1,"maker","book",x,0.5,0.01,1.,14.),14.);
    }

    #[test]
    fn role_overrides_are_independent_atomic_and_keep_stable_rank() {
        let mut s = Selection::default();
        s.configure("collect", "", 0.25, 42, true).unwrap();
        s.mode = Mode::Causal; s.model = Some(m());
        s.configure_role_strengths(Some(1.0), Some(0.0)).unwrap();
        for _ in 0..2 {
            for role in ["maker", "taker"] {
                s.select("owner", "order", "a", 1, role, "trade", [0.;N], 0.5, 0.01, 1., 10.);
            }
        }
        let rows: Vec<_> = s.drain().collect();
        assert_eq!(rows[0].probability, 0.5); assert_eq!(rows[1].probability, 1.);
        assert_eq!(rows[0].kept_qty, rows[2].kept_qty);
        assert_eq!(rows[1].kept_qty, 10.);
        assert!(s.configure_role_strengths(Some(0.0), Some(f64::NAN)).is_err());
        assert_eq!(s.role_strengths, [Some(1.0), Some(0.0)]);
        s.configure_role_strengths(None, None).unwrap();
        s.select("owner", "order", "a", 1, "maker", "trade", [0.;N], 0.5, 0.01, 1., 10.);
        assert_eq!(s.drain().next().unwrap().probability, 0.875);
    }

    #[test]
    #[ignore = "focused release benchmark; prints distributions, no wall-clock gate"]
    fn role_selection_latency_distribution() {
        use std::time::Instant;
        for (name, overrides) in [("shared", [None, None]), ("roles", [Some(0.5), Some(0.0)]), ("channels", [Some(0.5),Some(0.5)])] {
            let mut s = Selection::default();
            s.configure("collect", "", 0.25, 42, true).unwrap();
            s.mode = Mode::Causal; s.model = Some(m());
            if name=="channels" {
                let model=s.model.as_mut().unwrap();model.schema_version=2;
                model.maker_trade=Some(ChannelModel {fill:model.maker.clone(),quantity:Some([0.;N]),quantity_markout_weight:0.});
                model.taker_sweep=Some(ChannelModel {fill:model.taker.clone(),quantity:Some([0.;N]),quantity_markout_weight:0.});
            }
            s.configure_role_strengths(overrides[0], overrides[1]).unwrap();
            let mut times = Vec::with_capacity(100000);
            for i in 0..100000 {
                let start = Instant::now();
                std::hint::black_box(s.select("owner", "order", "a", i, if i%2==0 {"maker"} else {"taker"}, if i%2==0 {"trade"} else {"sweep"}, [0.;N], 0.5, 0.01, 1., 10.));
                times.push(start.elapsed().as_nanos());
                if i%128==127 { s.drain().for_each(drop); }
            }
            s.drain().for_each(drop); times.sort_unstable();
            println!("{name}: n=100000 select+bounded_audit_ns p50={} p99={} p999={} max={} queue_high_water={} overflow={}",times[50000],times[99000],times[99900],times[99999],s.stats.audit_high_water,s.stats.audit_overflows);
        }
    }

    #[test]
    fn causal_ignores_future_and_collect_is_identity() {
        let mut s = Selection::default();
        s.mode = Mode::Causal;
        s.model = Some(m());
        s.strength = 1.;
        s.audit = true;
        s.rows = VecDeque::with_capacity(CAPACITY);
        let x = [0.; N];
        s.set_forward("a", 1, Some(0.9));
        let a = s.select("one", "o", "a", 1, "maker", "trade", x, 0.5, 0.01, 1., 10.);
        s.set_forward("a", 1, Some(0.1));
        let b = s.select("one", "o", "a", 1, "maker", "trade", x, 0.5, 0.01, 1., 10.);
        assert_eq!(a, b);
        assert!(s
            .drain()
            .all(|r| r.forward_mid.is_none() && r.probability == 0.5));
        s.mode = Mode::Collect;
        assert_eq!(
            s.select("one", "o", "a", 1, "maker", "book", x, 0.5, 0.01, 1., 10.),
            10.
        );
    }
    #[test]
    fn stable_rank_owner_isolation_and_no_quantity_creation() {
        assert_eq!(uniform("a", "o", "maker", 1), uniform("a", "o", "maker", 1));
        assert_ne!(uniform("a", "o", "maker", 1), uniform("b", "o", "maker", 1));
        let mut s = Selection::default();
        s.mode = Mode::Forward;
        s.model = Some(m());
        s.strength = 1.;
        s.set_forward("a", 1, Some(0.8));
        for q in [0., 1., 14.] {
            let v = s.select(
                "a", "o", "a", 1, "maker", "trade", [0.; N], 0.5, 0.01, 1., q,
            );
            assert!(v == 0. || v == q);
        }
        assert_eq!(s.stats.candidates, 2);
    }
    #[test]
    #[should_panic(expected = "selection audit overflow")]
    fn overflow_fails_closed() {
        let mut s = Selection::default();
        s.configure("collect", "", 0., 42, true).unwrap();
        for _ in 0..=CAPACITY {
            s.select(
                "a", "o", "a", 1, "maker", "trade", [0.; N], 0.5, 0.01, 1., 1.,
            );
        }
    }
}
