use std::collections::VecDeque;
use std::sync::Mutex;

use futures::{Async, Future, Poll};
use futures::task::{self, Task};

/// The `ReturnSlot` through which the object can be returned to its
/// `OrderedCheckout` once the consumer is done with it. If a consumer
/// fails to return the object, no other consumer can ever proceed.
#[derive(Debug)]
pub struct ReturnSlot<'checkout, T>
    where T: 'checkout
{
    checkout: &'checkout OrderedCheckout<T>,
}

impl<'checkout, T> ReturnSlot<'checkout, T>
    where T: 'checkout
{
    /// Return the checked out item, consuming this `ReturnSlot`.
    pub fn return_(self, item: T) {
        let mut inner = self.checkout.inner.lock().unwrap();
        assert!(inner.data.is_none());
        inner.data = Some(item);
        if let Some(next) = inner.queue.pop_front() {
            next.notify();
        }
    }
}

/// The future that can be polled to receive the object.
/// To be hidden behind `impl Trait` once its stable.
#[derive(Debug)]
pub struct OrderedCheckoutFuture<'checkout, T>
    where T: 'checkout
{
    checkout: &'checkout OrderedCheckout<T>,
}

impl<'checkout, T> Future for OrderedCheckoutFuture<'checkout, T>
    where T: 'checkout
{
    type Item = (ReturnSlot<'checkout, T>, T);
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut inner = self.checkout.inner.lock().unwrap();
        if let Some(data) = inner.data.take() {
            let slot = ReturnSlot {
                checkout: self.checkout
            };
            return Ok(Async::Ready((slot, data)));
        }

        inner.queue.push_back(task::current());
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
struct OrderedCheckoutInner<T> {
    data: Option<T>,
    queue: VecDeque<Task>,
}

/// The `OrderedCheckout` object allows multiple consumers to check out and
/// return an object, queueing consumers when the object is not available and
/// waking them in order when it becomes available.
#[derive(Debug)]
pub struct OrderedCheckout<T> {
    inner: Mutex<OrderedCheckoutInner<T>>,
}

impl<T> OrderedCheckout<T> {
    /// Construct a new `OrderedCheckout` with the provided underlying object.
    pub fn new(item: T) -> OrderedCheckout<T> {
        OrderedCheckout {
            inner: Mutex::new(OrderedCheckoutInner {
                data: Some(item),
                queue: VecDeque::new(),
            })
        }
    }

    /// Begin the process of checking out an object. For efficiency, the future
    /// does not join the queue until it is polled. This can affect the order
    /// in which the object is received.
    pub fn checkout<'me>(&'me self) -> OrderedCheckoutFuture<'me, T> {
        OrderedCheckoutFuture {
            checkout: self,
        }
    }

    /// Consume this `OrderedCheckout` and return its underlying object, if any.
    /// Since this consumes the `OrderedCheckout` it is guaranteed that there are
    /// no outstanding waiters. Because a consumer can fail to return the object
    /// this may return None.
    pub fn into_inner(self) -> Option<T> {
        self.inner.into_inner().unwrap().data
    }
}

#[test]
fn test_checkout_return() {
    use std::cell::RefCell;

    let data: u32 = 42;
    let library = OrderedCheckout::new(data);

    let f2_seen_data = RefCell::new(false);
    {
        let f2_seen_data_ref = &f2_seen_data;
        let f1 = library.checkout()
            .map(|(ret, data)| {
                assert_eq!(*f2_seen_data_ref.borrow(), false);
                assert_eq!(data, 42);
                (ret, 13)
            })
            .map(|(ret, data)| ret.return_(data));
        let f2 = library.checkout()
            .map(|(ret, data)| {
                assert_eq!(*f2_seen_data_ref.borrow(), false);
                assert_eq!(data, 13);
                *f2_seen_data_ref.borrow_mut() = true;
                ret.return_(0)
            });
        f1.wait().unwrap();
        f2.wait().unwrap();
    }

    assert_eq!(f2_seen_data.into_inner(), true);
    assert_eq!(library.into_inner(), Some(0));
}

#[test]
fn test_checkout_return_queued() {
    use std::cell::RefCell;

    use futures::future::ok;
    use futures::sync::oneshot::channel;

    let data: u32 = 42;
    let library = OrderedCheckout::new(data);
    let (tx1, rx1) = channel();
    let (tx2, mut rx2) = channel();
    let f2_seen_data = RefCell::new(false);
    ok::<_, ()>(()).and_then(|_| {
        let f1_seen_data = RefCell::new(false);
        let f1_seen_data_ref = &f1_seen_data;
        let f2_seen_data_ref = &f2_seen_data;
        let mut f1 = library.checkout()
            .map(|(ret, data)| {
                *f1_seen_data_ref.borrow_mut() = true;
                (ret, data)
            })
            .join(rx1.map_err(|_| ()))
            .map(|((ret, data), _)| {
                assert_eq!(*f2_seen_data_ref.borrow(), false);
                assert_eq!(data, 42);
                (ret, 13)
            })
            .map(|(ret, data)| ret.return_(data));
        let mut f2 = library.checkout()
            .map(|(ret, data)| {
                assert_eq!(*f2_seen_data_ref.borrow(), false);
                assert_eq!(data, 13);
                *f2_seen_data_ref.borrow_mut() = true;
                ret.return_(0);
                tx2.send(()).unwrap()
            });
        assert_eq!(f1.poll(), Ok(Async::NotReady));
        assert_eq!(*f1_seen_data_ref.borrow(), true);
        assert_eq!(f2.poll(), Ok(Async::NotReady));
        tx1.send(()).unwrap();
        assert_eq!(f2.poll(), Ok(Async::NotReady));
        f1.wait().unwrap();
        assert_eq!(rx2.poll(), Ok(Async::NotReady));
        f2.wait().unwrap();
        rx2.wait().unwrap();
        Ok(())
    }).wait().unwrap();

    assert_eq!(f2_seen_data.into_inner(), true);
    assert_eq!(library.into_inner(), Some(0));
}
