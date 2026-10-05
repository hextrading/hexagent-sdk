//! Dispatcher-local CPU work hints. These never grant admission. One cumulative
//! completion snapshot per physical owner makes replacement/duplicates harmless.
use hexagent_runtime::latest_snapshot::{self, Publisher, Receiver};
use std::{cell::RefCell, rc::Rc};

#[derive(Default)]
pub(crate) struct Schedule {
    lanes: Vec<Lane>,
}
struct Lane {
    core: usize,
    issued: u64,
    done: u64,
    rx: Receiver<u64>,
}
#[derive(Clone)]
pub(crate) struct Route {
    schedule: Rc<RefCell<Schedule>>,
    index: usize,
}
impl Schedule {
    pub(crate) fn register(schedule: &Rc<RefCell<Self>>, core: usize) -> (Route, Completion) {
        let (tx, rx) = latest_snapshot::channel();
        let mut state = schedule.borrow_mut();
        let index = state.lanes.len();
        state.lanes.push(Lane {
            core,
            issued: 0,
            done: 0,
            rx,
        });
        (
            Route {
                schedule: schedule.clone(),
                index,
            },
            Completion { tx, count: 0 },
        )
    }
}
impl Route {
    pub(crate) fn refresh(&self) {
        for lane in &mut self.schedule.borrow_mut().lanes {
            if let Ok(done) = lane.rx.try_recv() {
                lane.done = lane.done.max(done).min(lane.issued);
            }
        }
    }
    pub(crate) fn core(&self) -> usize {
        self.schedule.borrow().lanes[self.index].core
    }
    pub(crate) fn pending_on_core(&self) -> u64 {
        let state = self.schedule.borrow();
        let core = state.lanes[self.index].core;
        state
            .lanes
            .iter()
            .filter(|lane| lane.core == core)
            .map(|lane| lane.issued.saturating_sub(lane.done))
            .sum()
    }
    pub(crate) fn issued(&self) {
        self.schedule.borrow_mut().lanes[self.index].issued += 1;
    }
}
pub(crate) struct Completion {
    tx: Publisher<u64>,
    count: u64,
}
impl Completion {
    pub(crate) fn guard(&mut self) -> Guard<'_> {
        Guard(Some(self))
    }
}
pub(crate) struct Guard<'a>(Option<&'a mut Completion>);
impl Guard<'_> {
    pub(crate) fn finish(&mut self) {
        if let Some(completion) = self.0.take() {
            completion.count += 1;
            completion.tx.publish(completion.count);
        }
    }
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_core_load_ends_before_http_and_replaced_snapshots_do_not_lose_completions() {
        let s = Rc::new(RefCell::new(Schedule::default()));
        let (a, mut ac) = Schedule::register(&s, 5);
        let (b, mut bc) = Schedule::register(&s, 5);
        let (c, _cc) = Schedule::register(&s, 14);
        a.issued();
        b.issued();
        assert_eq!(a.pending_on_core(), 2);
        assert_eq!(c.pending_on_core(), 0);
        ac.guard().finish();
        a.refresh();
        assert_eq!(b.pending_on_core(), 1);
        // HTTP may still be outstanding; it does not retain CPU preparation work.
        b.issued();
        bc.guard().finish();
        bc.guard().finish();
        b.refresh();
        b.refresh();
        assert_eq!(a.pending_on_core(), 0);
    }
    #[test]
    fn error_guard_finishes_once_and_other_schedulers_are_isolated() {
        let a = Rc::new(RefCell::new(Schedule::default()));
        let b = Rc::new(RefCell::new(Schedule::default()));
        let (ar, mut ac) = Schedule::register(&a, 5);
        let (br, _) = Schedule::register(&b, 5);
        ar.issued();
        {
            let mut guard = ac.guard();
            guard.finish();
            guard.finish();
        }
        ar.refresh();
        assert_eq!(ar.pending_on_core(), 0);
        assert_eq!(br.pending_on_core(), 0);
    }
}
