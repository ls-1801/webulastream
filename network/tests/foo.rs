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
fn start_and_stop_sender() {
    // TODO whyyyy stuck without subscriber?!
    tracing_subscriber::fmt().init();

    let service = sender::NetworkService::start(make_rt());

    service.register_channel(String::from("127.0.0.1:0"), String::from("bar")).unwrap();
    service.shutdown().unwrap();
}

#[test]
fn start_and_stop_receiver() {
    tracing_subscriber::fmt().init();

    let service = receiver::NetworkService::start(make_rt(), String::from("127.0.0.1:0"));

    service.register_channel(String::from("bar")).unwrap();
    service.shutdown().unwrap();
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
fn register_channel() {
    console_subscriber::init();

    let conn = String::from("127.0.0.1:13254");
    let chan = String::from("123");

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());
    let sender = sender::NetworkService::start(make_rt());

    sender.register_channel(conn.clone(), chan.clone()).unwrap();
    rceivr.register_channel(chan.clone()).unwrap();

    // wait a while so that channel is async-ly opened
    sleep(Duration::from_millis(100));

    sender.shutdown().unwrap();
    rceivr.shutdown().unwrap();
}

#[test]
fn send_and_receive() {
    tracing_subscriber::fmt().init();

    let conn = String::from("127.0.0.1:13254");
    let chan = String::from("123");

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());
    let sender = sender::NetworkService::start(make_rt());

    let sender_chan = sender.register_channel(conn.clone(), chan.clone()).unwrap();

    let t = make_tb(0);
    let msg = ChannelControlMessage::Data(t.clone());

    executor::block_on(sender_chan.send(msg)).unwrap();


    let rceivr_chan = rceivr.register_channel(chan.clone()).unwrap();
    let t1 = rceivr_chan.recv_blocking().unwrap();

    assert_eq!(t, t1);

    sender.shutdown().unwrap();
    rceivr.shutdown().unwrap();
}

#[test]
fn send_and_receive_1000() {
    tracing_subscriber::fmt().init();

    let conn = String::from("127.0.0.1:13254");
    let chan = String::from("123");

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());
    let sender = sender::NetworkService::start(make_rt());

    let sender_chan = sender.register_channel(conn.clone(), chan.clone()).unwrap();
    let rceivr_chan = rceivr.register_channel(chan.clone()).unwrap();

    let num_send_recvs = 1000;

    let mut send_tbs = Vec::new();

    for i in 0..num_send_recvs {
        let t = make_tb(i);
        send_tbs.push(t.clone());
        let msg = ChannelControlMessage::Data(t.clone());

        executor::block_on(sender_chan.send(msg)).unwrap();
    }


    for i in 0..num_send_recvs {
        let t = rceivr_chan.recv_blocking().unwrap();
        assert_eq!(send_tbs[i as usize], t);
    }

    sender.shutdown().unwrap();
    rceivr.shutdown().unwrap();
}

#[test]
fn send_recv_restart_receiver() {
    tracing_subscriber::fmt().init();

    let conn = String::from("127.0.0.1:13254");
    let chan = String::from("123");

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());
    let sender = sender::NetworkService::start(make_rt());

    let sender_chan = sender.register_channel(conn.clone(), chan.clone()).unwrap();

    let t = make_tb(1);
    let msg = ChannelControlMessage::Data(t.clone());

    executor::block_on(sender_chan.send(msg)).unwrap();

    let rceivr_chan = rceivr.register_channel(chan.clone()).unwrap();
    let t1 = rceivr_chan.recv_blocking().unwrap();
    assert_eq!(t, t1);

    std::mem::drop(rceivr);

    let rceivr = receiver::NetworkService::start(make_rt(), conn.clone());

    let t = make_tb(2);
    let msg = ChannelControlMessage::Data(t.clone());

    executor::block_on(sender_chan.send(msg)).unwrap();

    let rceivr_chan = rceivr.register_channel(chan.clone()).unwrap();
    let t1 = rceivr_chan.recv_blocking().unwrap();
    assert_eq!(t, t1);

    sender.shutdown().unwrap();
}
