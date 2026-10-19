//! Single-thread round-robin service of startup-bound physical actors.
//! The caller owns every actor, inbox and in-flight reply. `poll` MUST NOT wait:
//! a slow reply or a full outbox stays in that actor's bounded pending slot.
//! This cursor is scheduling only, never business admission or queue authority.

pub struct Cursor {
    next: usize,
    lanes: usize,
    budget: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Turn {
    pub visited: usize,
    pub progressed: usize,
}

impl Cursor {
    pub fn new(lanes: usize, budget: usize) -> Self {
        assert!((1..=64).contains(&lanes));
        assert!((1..=lanes).contains(&budget));
        Self { next: 0, lanes, budget }
    }

    /// At most one visit per actor, and at most `budget` successful transitions.
    /// A completely idle turn visits ALL actors; only then may the owner sleep.
    /// Arm the shared Wake, repeat this check, then wait to avoid lost wakeups.
    /// After a productive turn, yield to equal-priority completion workers.
    pub fn poll(&mut self, mut poll: impl FnMut(usize) -> bool) -> Turn {
        let mut turn = Turn::default();
        for _ in 0..self.lanes {
            let slot = self.next;
            self.next = (self.next + 1) % self.lanes;
            turn.visited += 1;
            if poll(slot) {
                turn.progressed += 1;
                if turn.progressed == self.budget { break; }
            }
        }
        turn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_turns_rotate_across_accounts_without_starvation() {
        let mut cursor = Cursor::new(9, 2);
        let mut visits = Vec::new();
        for _ in 0..9 {
            let turn = cursor.poll(|i| { visits.push(i); true });
            assert_eq!(turn, Turn { visited: 2, progressed: 2 });
        }
        assert_eq!(visits, (0..9).chain(0..9).collect::<Vec<_>>());
    }

    #[test]
    fn slow_reply_and_full_outbox_do_not_block_other_owners() {
        let mut cursor = Cursor::new(4, 2);
        let mut complete = [0; 4];
        for _ in 0..20 {
            cursor.poll(|i| {
                if i < 2 { return false; } // pending HTTP / retained full outbox
                complete[i] += 1;
                true
            });
        }
        assert_eq!(complete, [0, 0, 20, 20]);
        let idle = cursor.poll(|_| false);
        assert_eq!(idle, Turn { visited: 4, progressed: 0 });
    }

    #[test]
    fn empty_prefix_cannot_hide_last_lane_before_sleep() {
        let mut cursor = Cursor::new(12, 1);
        let turn = cursor.poll(|i| i == 11);
        assert_eq!(turn, Turn { visited: 12, progressed: 1 });
    }
}
