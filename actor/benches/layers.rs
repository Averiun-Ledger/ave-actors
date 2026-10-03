//! Layer split for the ask path: raw tokio mpsc round-trip vs the
//! full actor ask. The gap between them is the runner/handler overhead
//! (envelope, span, dispatch, oneshot reply).

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::sync::{mpsc, oneshot};

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("bench runtime")
}

fn bench_raw_mpsc_echo(c: &mut Criterion) {
    let rt = rt();
    let (tx, mut rx) = mpsc::channel::<(u64, oneshot::Sender<u64>)>(1024);
    rt.spawn(async move {
        while let Some((n, reply)) = rx.recv().await {
            let _ = reply.send(n + 1);
        }
    });
    c.bench_function("layers/raw_mpsc_echo", |b| {
        b.to_async(&rt).iter(|| async {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send((41, reply_tx)).await.expect("send");
            reply_rx.await.expect("reply");
        });
    });
}

enum Item {
    Msg,
    Barrier(oneshot::Sender<()>),
}

fn bench_raw_mpsc_tell(c: &mut Criterion) {
    let rt = rt();
    let (tx, mut rx) = mpsc::channel::<Item>(4096);
    rt.spawn(async move {
        while let Some(item) = rx.recv().await {
            if let Item::Barrier(reply) = item {
                let _ = reply.send(());
            }
        }
    });
    c.bench_function("layers/raw_mpsc_tell_x200", |b| {
        b.to_async(&rt).iter(|| async {
            for _ in 0..200 {
                tx.send(Item::Msg).await.expect("send");
            }
            // Barrier queued behind the tells: drained => all 200 seen.
            let (bar_tx, bar_rx) = oneshot::channel();
            tx.send(Item::Barrier(bar_tx)).await.expect("send");
            bar_rx.await.expect("barrier");
        });
    });
}

criterion_group!(benches, bench_raw_mpsc_echo, bench_raw_mpsc_tell,);
criterion_main!(benches);
