//! Startup-allocated actors polled by a current-thread runtime's ROOT future.
//!
//! A normal spawned task's remote waker enters Tokio's injection queue. The
//! root waker instead sets an atomic flag and unparks the I/O reactor. Each
//! producer transfers owned commands through its bounded mailbox, then wakes
//! that immutable root waker. No task creation or waiter registration per command.
//! The driver is the sole owner of all actor futures. Registration is startup
//! work, bounded by `capacity`; no shared strategy/account authority lives here.
use crate::{poll_channel, try_queue::TryQueue};
use crossbeam_channel::{TryRecvError, TrySendError};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    task::{Context, Poll, Waker},
};

type Actor = Pin<Box<dyn Future<Output = ()> + Send>>;
struct Shared {
    incoming: TryQueue<Actor>,
    waker: OnceLock<Waker>,
    alive: AtomicBool,
    reserved: AtomicUsize,
    capacity: usize,
}
impl Shared {
    fn wake(&self) {
        if let Some(waker) = self.waker.get() {
            waker.wake_by_ref();
        }
        // Before the first root poll no parking is possible; that poll drains
        // registrations after installing the waker, closing the startup race.
    }
}
#[derive(Clone)]
pub struct Registry(Arc<Shared>);
pub struct Driver {
    shared: Arc<Shared>,
    actors: Vec<Actor>,
}

pub fn driver(capacity: usize) -> (Registry, Driver) {
    let shared = Arc::new(Shared {
        incoming: TryQueue::new(capacity),
        waker: OnceLock::new(),
        alive: AtomicBool::new(true),
        reserved: AtomicUsize::new(0),
        capacity,
    });
    let actors = Vec::with_capacity(capacity);
    (Registry(shared.clone()), Driver { shared, actors })
}

pub struct Sender<T> {
    tx: Option<poll_channel::Sender<T>>,
    shared: Arc<Shared>,
}
pub struct Inbox<T> {
    rx: poll_channel::Receiver<T>,
    yield_next: bool,
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            shared: self.shared.clone(),
        }
    }
}
impl Registry {
    /// Startup only. Capacity exhaustion is explicit; never fall back to a
    /// per-command spawn. Each actor owns one mailbox and drains it in order.
    pub fn register<T: Send + 'static, F: Future<Output = ()> + Send + 'static>(
        &self,
        capacity: usize,
        make: impl FnOnce(Inbox<T>) -> F,
    ) -> Result<Sender<T>, &'static str> {
        if !self.0.alive.load(Ordering::Acquire) {
            return Err("I/O root stopped");
        }
        self.0
            .reserved
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.0.capacity).then_some(n + 1)
            })
            .map_err(|_| "I/O actor capacity exhausted")?;
        let (tx, rx) = poll_channel::bounded(capacity);
        let actor: Actor = Box::pin(make(Inbox {
            rx,
            yield_next: false,
        }));
        if self.0.incoming.try_push(actor).is_err() {
            self.0.reserved.fetch_sub(1, Ordering::AcqRel);
            return Err("I/O actor registration busy; retry outside hot path");
        }
        if !self.0.alive.load(Ordering::Acquire) {
            while self.0.incoming.try_pop().is_some() {}
            return Err("I/O root stopped during registration");
        }
        self.0.wake();
        Ok(Sender {
            tx: Some(tx),
            shared: self.0.clone(),
        })
    }
}
impl<T> Sender<T> {
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if !self.shared.alive.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }
        self.tx.as_ref().unwrap().try_send(value)?;
        self.tx.as_ref().unwrap().discard_if_disconnected();
        self.shared.wake();
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.tx.as_ref().unwrap().len()
    }
    pub fn is_empty(&self) -> bool {
        self.tx.as_ref().unwrap().is_empty()
    }
    pub fn is_disconnected(&self) -> bool {
        !self.shared.alive.load(Ordering::Acquire) || self.tx.as_ref().unwrap().is_disconnected()
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Publish sender closure before waking the root.
        drop(self.tx.take());
        self.shared.wake();
    }
}
impl<T> Inbox<T> {
    pub async fn recv(&mut self) -> Option<T> {
        futures_util::future::poll_fn(|cx| {
            // At most one completed command per root poll, even for an actor
            // whose operation completes synchronously. Other owners and Tokio
            // socket/timer tasks must get a turn under a continuous burst.
            if self.yield_next {
                self.yield_next = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            match self.rx.try_recv() {
                Ok(value) => {
                    self.yield_next = true;
                    Poll::Ready(Some(value))
                }
                Err(TryRecvError::Disconnected) => Poll::Ready(None),
                Err(TryRecvError::Empty) => Poll::Pending,
            }
        })
        .await
    }
}
impl<T> Drop for Inbox<T> {
    fn drop(&mut self) {
        self.rx.close_and_discard();
    }
}
impl Future for Driver {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let _ = this.shared.waker.get_or_init(|| cx.waker().clone());
        while let Some(actor) = this.shared.incoming.try_pop() {
            assert!(
                this.actors.len() < this.shared.capacity,
                "reserved actor capacity"
            );
            this.actors.push(actor);
        }
        let mut index = 0;
        while index < this.actors.len() {
            let actor = &mut this.actors[index];
            // Same isolation as a spawned Tokio task: an actor panic retires
            // its inbox/replies, not the reactor or sibling connection owners.
            let poll =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.as_mut().poll(cx)));
            if !matches!(poll, Ok(Poll::Pending)) {
                drop(this.actors.swap_remove(index));
                this.shared.reserved.fetch_sub(1, Ordering::AcqRel);
            } else {
                index += 1;
            }
        }
        Poll::Pending
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        self.shared.alive.store(false, Ordering::Release);
        while self.shared.incoming.try_pop().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct Running {
        registry: Registry,
        stop: Arc<AtomicBool>,
        join: Option<std::thread::JoinHandle<()>>,
    }
    impl Running {
        fn new(capacity: usize) -> Self {
            let (registry, mut driver) = driver(capacity);
            let stop = Arc::new(AtomicBool::new(false));
            let shutdown = stop.clone();
            let join = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(futures_util::future::poll_fn(move |cx| {
                    if shutdown.load(Ordering::Acquire) {
                        Poll::Ready(())
                    } else {
                        Pin::new(&mut driver).poll(cx)
                    }
                }));
            });
            Self {
                registry,
                stop,
                join: Some(join),
            }
        }
    }
    impl Drop for Running {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            self.registry.0.wake();
            self.join.take().unwrap().join().unwrap();
        }
    }

    #[test]
    fn bounded_registration_and_mailbox_preserve_values_before_first_poll() {
        let (registry, driver) = driver(1);
        let tx = registry
            .register(
                2,
                |mut rx| async move { while rx.recv().await.is_some() {} },
            )
            .unwrap();
        assert!(registry.register::<u64, _>(1, |_| async {}).is_err());
        tx.try_send(11).unwrap();
        tx.try_send(12).unwrap();
        assert_eq!(tx.try_send(13), Err(TrySendError::Full(13)));
        assert_eq!(tx.len(), 2);
        drop(driver);
        assert_eq!(tx.try_send(14), Err(TrySendError::Disconnected(14)));
        assert!(registry.register::<u64, _>(1, |_| async {}).is_err());
    }

    #[test]
    fn parked_root_wakes_for_commands_and_last_sender_drop() {
        let running = Running::new(2);
        let (results, rx) = std::sync::mpsc::sync_channel(2);
        let tx = running
            .registry
            .register(2, |mut inbox| async move {
                while let Some(value) = inbox.recv().await {
                    results.send(value).unwrap();
                }
                results.send(9999).unwrap();
            })
            .unwrap();
        for n in 0..1000 {
            // Exercise idle wake and the reply-to-next-poll/park race repeatedly.
            tx.try_send(n).unwrap();
            assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), n);
            if n % 20 == 0 {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        drop(tx);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 9999);
    }

    #[test]
    fn abandoned_actor_releases_all_reply_guards_while_sender_stays_alive() {
        struct Reply(std::sync::mpsc::SyncSender<()>);
        impl Drop for Reply {
            fn drop(&mut self) {
                self.0.send(()).unwrap();
            }
        }
        let (registry, driver) = driver(1);
        let (released, received) = std::sync::mpsc::sync_channel(2);
        let sender = registry
            .register(2, |mut inbox: Inbox<Reply>| async move {
                while inbox.recv().await.is_some() {}
            })
            .unwrap();
        assert!(sender.try_send(Reply(released.clone())).is_ok());
        assert!(sender.try_send(Reply(released)).is_ok());
        drop(driver);
        // Both reply guards must drop now; a retained producer cannot keep
        // completion consumers hanging after an actor/driver failure.
        for _ in 0..2 {
            received.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        assert!(sender.is_disconnected());
        assert!(sender.is_empty());
    }

    #[test]
    fn slow_owner_timer_and_panicked_owner_do_not_block_other_instances() {
        let running = Running::new(3);
        let (results, rx) = std::sync::mpsc::sync_channel(8);
        let slow_results = results.clone();
        let slow = running
            .registry
            .register(2, |mut inbox| async move {
                while let Some(n) = inbox.recv().await {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    slow_results.send((1, n)).unwrap();
                }
            })
            .unwrap();
        let fast = running
            .registry
            .register(2, |mut inbox| async move {
                while let Some(n) = inbox.recv().await {
                    results.send((2, n)).unwrap();
                }
            })
            .unwrap();
        let broken = running
            .registry
            .register(1, |mut inbox| async move {
                let _: Option<u64> = inbox.recv().await;
                panic!("injected owner failure");
            })
            .unwrap();
        broken.try_send(1).unwrap();
        slow.try_send(1).unwrap();
        slow.try_send(2).unwrap();
        fast.try_send(7).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), (2, 7));
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), (1, 1));
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), (1, 2));
        assert!(broken.is_disconnected());
        assert!(rx.try_recv().is_err(), "no duplicate delivery");
        // Reuse a retired actor slot with a fresh inbox, never an old command.
        let (out, received) = std::sync::mpsc::sync_channel(1);
        let replacement = running
            .registry
            .register(1, |mut inbox| async move {
                out.send(inbox.recv().await.unwrap()).unwrap();
            })
            .unwrap();
        replacement.try_send(9).unwrap();
        assert_eq!(received.recv_timeout(Duration::from_secs(2)).unwrap(), 9);
    }
}
