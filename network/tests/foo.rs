use std::time::Duration;

use nes_network::{receiver, sender};
use tokio::time::sleep;

#[test]
fn start_and_stop_sender() -> Result<(), String> {
    // TODO whyyyy stuck without subscriber?!
    tracing_subscriber::fmt().init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let service = sender::NetworkService::start(rt);

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
