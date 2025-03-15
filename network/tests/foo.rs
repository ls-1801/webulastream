use std::{thread::sleep, time::Duration, vec};

use futures::executor;
use nes_network::{protocol::TupleBuffer, receiver, sender::{self, ChannelControlMessage}};

#[test]
fn start_and_stop_sender() -> Result<(), String> {
    // TODO whyyyy stuck without subscriber?!
    // tracing_subscriber::fmt().init();
    console_subscriber::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let service = sender::NetworkService::start(rt);

    sleep(Duration::from_secs(10));

    service.register_channel(String::from("foo"), String::from("bar")).map_err(|e| e.to_string())?;
    service.shutdown().map_err(|e| e.to_string())?;

    Ok(())
}

#[test]
fn start_and_stop_receiver() -> Result<(), String> {
    tracing_subscriber::fmt().init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let service = receiver::NetworkService::start(rt, String::from("127.0.0.1:0"));

    service.register_channel(String::from("bar")).map_err(|e| e.to_string())?;
    service.shutdown().map_err(|e| e.to_string())?;

    Ok(())
}

#[test]
fn send_and_receive() -> Result<(), String> {
    // tracing_subscriber::fmt().init();
    console_subscriber::init();

    let conn = String::from("127.0.0.1:13254");
    let chan = String::from("123");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let rceivr = receiver::NetworkService::start(rt, conn.clone());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    let sender = sender::NetworkService::start(rt);

    let sender_chan = sender.register_channel(conn.clone(), chan.clone()).map_err(|e| e.to_string())?;

    let t = TupleBuffer {
            sequence_number: 1,
            watermark: 1,
            origin_id: 1,
            chunk_number: 1,
            number_of_tuples: 1,
            last_chunk: false,
            data: vec![b'1', b'3', b'1', b'2'],
            child_buffers: Vec::new()
    };

    let msg = ChannelControlMessage::Data(t.clone());

    executor::block_on(sender_chan.send(msg)).map_err(|e| e.to_string())?;


    let rceivr_chan = rceivr.register_channel(chan.clone()).map_err(|e| e.to_string())?;
    let t1 = rceivr_chan.recv_blocking().map_err(|e| e.to_string())?;

    assert_eq!(t, t1);

    sender.shutdown().map_err(|e| e.to_string())?;
    rceivr.shutdown().map_err(|e| e.to_string())?;

    Ok(())
}
