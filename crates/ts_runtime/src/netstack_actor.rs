use std::sync::Arc;

use kameo::{
    actor::{ActorRef, Spawn},
    message::{Context, Message},
};
use netstack::{
    HasChannel,
    netcore::{Channel, NetstackControl},
};
use tokio::sync::Mutex;
use ts_dataplane::async_tokio::{FromOverlay, Rx, ToOverlay, Tx};
use ts_packet::PacketMut;
use ts_transport::OverlayTransportId;

use crate::{Error, env::Env, task::Task};

pub struct NetstackActor {
    channel: Channel,
}

impl kameo::Actor for NetstackActor {
    type Args = (
        Env,
        netstack::netcore::Config,
        OverlayTransportId,
        Tx<FromOverlay>,
        Arc<Mutex<Rx<ToOverlay>>>,
    );
    type Error = Error;

    async fn on_start(
        (env, config, id, netstack_up, netstack_down): Self::Args,
        slf: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        env.subscribe::<Arc<ts_control::StateUpdate>>(&slf).await?;

        let (
            mut netstack,
            netstack::WakingPipe {
                rx: mut netstack_down_rx,
                tx: netstack_down_tx,
            },
        ) = netstack::piped(config);
        let channel = netstack.command_channel();

        Task::spawn_link(&slf, async move {
            netstack.run_tokio().await;
        })
        .await;

        Task::spawn_link(&slf, async move {
            while let Some(buf) = netstack_down_rx.recv_async().await {
                let packets = outgoing_batch(buf, &mut netstack_down_rx);
                if netstack_up.send((id, packets)).is_err() {
                    break;
                }
            }

            tracing::warn!("netstack downlink shut down!");
        })
        .await;

        Task::spawn_link(&slf, async move {
            // NetstackActor is supervised, so the same netstack down channel may be handed to
            // another actor instance if this one panics. We hold this mutex open indefinitely,
            // relying on the Drop of this future to clean it up when the owning joinset drops.
            let mut netstack_down = netstack_down.lock().await;

            while let Some(bufs) = netstack_down.recv().await {
                for buf in bufs {
                    let buf: PacketMut = buf;
                    netstack_down_tx.send_async(buf.as_ref()).await;
                }
            }

            tracing::warn!("netstack uplink shut down!");
        })
        .await;

        env.register(None, &slf).await?;

        Ok(Self { channel })
    }
}

/// Get a netstack command channel.
#[derive(Debug, Copy, Clone)]
pub struct GetChannel;

impl Message<GetChannel> for NetstackActor {
    type Reply = (Channel,);

    async fn handle(
        &mut self,
        _: GetChannel,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        (self.channel.clone(),)
    }
}

impl Message<Arc<ts_control::StateUpdate>> for NetstackActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: Arc<ts_control::StateUpdate>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) {
        let Some(self_node) = &msg.node else {
            return;
        };

        tracing::debug!(new_tailnet_ips = ?self_node.tailnet_address);

        if let Err(e) = self
            .channel
            .set_ips([
                self_node.tailnet_address.ipv4.addr().into(),
                self_node.tailnet_address.ipv6.addr().into(),
            ])
            .await
        {
            tracing::error!(error = %e, "setting netstack ips");
        }
    }
}

// Bound each handoff so the dataplane can service incoming packets and other work
// between batches. Drain only packets already available; never wait to fill a batch.
const OUTBOUND_BATCH_SIZE: usize = 32;

fn outgoing_batch(
    first: bytes::Bytes,
    receiver: &mut netstack::WakingPipeReceiver,
) -> Vec<PacketMut> {
    let mut packets = Vec::with_capacity(OUTBOUND_BATCH_SIZE);
    packets.push(packet_from_bytes(first));
    while packets.len() < OUTBOUND_BATCH_SIZE {
        let Some(buf) = receiver.try_recv() else {
            break;
        };
        packets.push(packet_from_bytes(buf));
    }
    packets
}

fn packet_from_bytes(buf: bytes::Bytes) -> PacketMut {
    // The pipe usually transfers the sole reference to a frozen BytesMut. Reuse
    // that allocation; copy only when somebody else still holds the buffer.
    buf.try_into_mut()
        .unwrap_or_else(|buf| bytes::BytesMut::from(buf.as_ref()))
        .into()
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use super::{OUTBOUND_BATCH_SIZE, outgoing_batch, packet_from_bytes};

    #[test]
    fn output_batches_preserve_order_and_drain_after_close() {
        let (mut receiver, sender) = netstack::WakingPipe::unbounded();
        let count = OUTBOUND_BATCH_SIZE * 2 + 1;
        for n in 0..count {
            sender.tx.send(&[n as u8]);
        }
        drop(sender);
        let mut seen = Vec::new();
        let mut lengths = Vec::new();
        while let Some(first) = receiver.rx.try_recv() {
            let packets = outgoing_batch(first, &mut receiver.rx);
            lengths.push(packets.len());
            seen.extend(packets.iter().map(|p| p[0]));
        }
        assert_eq!(lengths, [OUTBOUND_BATCH_SIZE, OUTBOUND_BATCH_SIZE, 1]);
        assert_eq!(seen, (0..count as u8).collect::<Vec<_>>());
    }

    #[test]
    fn unique_output_buffer_reuses_its_allocation() {
        let buf = BytesMut::from(&b"packet"[..]).freeze();
        let ptr = buf.as_ptr();
        let packet = packet_from_bytes(buf);
        assert_eq!(packet.as_ref(), b"packet");
        assert_eq!(packet.as_ptr(), ptr);
    }

    #[test]
    fn shared_output_buffer_is_copied_before_mutation() {
        let original = Bytes::from(Vec::from(&b"packet"[..]));
        let mut packet = packet_from_bytes(original.clone());
        packet[0] = b'P';
        assert_eq!(original.as_ref(), b"packet");
        assert_eq!(packet.as_ref(), b"Packet");
    }
}
