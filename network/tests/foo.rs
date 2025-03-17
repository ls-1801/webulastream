use std::{thread::sleep, time::Duration, vec};

use futures::executor;
use nes_network::{protocol::TupleBuffer, receiver, sender::{self, ChannelControlMessage}};
use tokio::runtime::Runtime;

fn make_rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

#[test]
fn start_and_stop_sender() -> Result<(), String> {
    // TODO whyyyy stuck without subscriber?!
    tracing_subscriber::fmt().init();

    let service = sender::NetworkService::start(make_rt());

    sleep(Duration::from_secs(10));

    service.register_channel(String::from("foo"), String::from("bar")).map_err(|e| e.to_string())?;
    service.shutdown().map_err(|e| e.to_string())?;

    Ok(())
}

#[test]
fn start_and_stop_receiver() -> Result<(), String> {
    tracing_subscriber::fmt().init();

    let service = receiver::NetworkService::start(make_rt(), String::from("127.0.0.1:0"));

    service.register_channel(String::from("bar")).map_err(|e| e.to_string())?;
    service.shutdown().map_err(|e| e.to_string())?;

    Ok(())
}

fn make_tb(seq_no: u64) -> TupleBuffer {
    TupleBuffer {
            sequence_number: seq_no,
            watermark: 1,
            origin_id: 1,
            chunk_number: 1,
            number_of_tuples: 1,
            last_chunk: false,
            data: vec![b'1', b'3', b'1', b'2'],
            child_buffers: Vec::new()
    }
}

#[test]
fn send_and_receive() -> Result<(), String> {
    tracing_subscriber::fmt().init();

    let conn = String::from("127.0.0.1:13254");
    let chan = String::from("123");

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());
    let sender = sender::NetworkService::start(make_rt());

    let sender_chan = sender.register_channel(conn.clone(), chan.clone()).map_err(|e| e.to_string())?;

    let t = make_tb(0);
    let msg = ChannelControlMessage::Data(t.clone());

    executor::block_on(sender_chan.send(msg)).map_err(|e| e.to_string())?;


    let rceivr_chan = rceivr.register_channel(chan.clone()).map_err(|e| e.to_string())?;
    let t1 = rceivr_chan.recv_blocking().map_err(|e| e.to_string())?;

    assert_eq!(t, t1);

    sender.shutdown().map_err(|e| e.to_string())?;
    rceivr.shutdown().map_err(|e| e.to_string())?;

    Ok(())
}

#[test]
fn send_and_receive_1000() -> Result<(), String> {
    tracing_subscriber::fmt().init();

    let conn = String::from("127.0.0.1:13254");
    let chan = String::from("123");

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());
    let sender = sender::NetworkService::start(make_rt());

    let sender_chan = sender.register_channel(conn.clone(), chan.clone()).map_err(|e| e.to_string())?;
    let rceivr_chan = rceivr.register_channel(chan.clone()).map_err(|e| e.to_string())?;

    let num_send_recvs = 1000;

    let mut send_tbs = Vec::new();

    for i in 0..num_send_recvs {
        let t = make_tb(i);
        send_tbs.push(t.clone());
        let msg = ChannelControlMessage::Data(t.clone());

        executor::block_on(sender_chan.send(msg)).map_err(|e| e.to_string())?;
    }


    for i in 0..num_send_recvs {
        let t = rceivr_chan.recv_blocking().map_err(|e| e.to_string())?;
        assert_eq!(send_tbs[i as usize], t);
    }

    sender.shutdown().map_err(|e| e.to_string())?;
    rceivr.shutdown().map_err(|e| e.to_string())?;

    Ok(())
}

#[test]
fn send_recv_restart_receiver() -> Result<(), String> {
    tracing_subscriber::fmt().init();

    let conn = String::from("127.0.0.1:13254");
    let chan = String::from("123");

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());
    let sender = sender::NetworkService::start(make_rt());

    let sender_chan = sender.register_channel(conn.clone(), chan.clone()).map_err(|e| e.to_string())?;

    let t = make_tb(1);
    let msg = ChannelControlMessage::Data(t.clone());

    executor::block_on(sender_chan.send(msg)).map_err(|e| e.to_string())?;

    let rceivr_chan = rceivr.register_channel(chan.clone()).map_err(|e| e.to_string())?;
    let t1 = rceivr_chan.recv_blocking().map_err(|e| e.to_string())?;
    assert_eq!(t, t1);

    std::mem::drop(rceivr);

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());

    let t = make_tb(2);
    let msg = ChannelControlMessage::Data(t.clone());

    executor::block_on(sender_chan.send(msg)).map_err(|e| e.to_string())?;

    let rceivr_chan = rceivr.register_channel(chan.clone()).map_err(|e| e.to_string())?;
    let t1 = rceivr_chan.recv_blocking().map_err(|e| e.to_string())?;
    assert_eq!(t, t1);

    sender.shutdown().map_err(|e| e.to_string())?;

    Ok(())
}
