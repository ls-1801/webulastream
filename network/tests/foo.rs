use std::time::Duration;

use nes_network::sender::NetworkService;
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

    let service = NetworkService::start(rt);

    match service.register_channel(String::from("foo"), String::from("bar")) {
        Ok(_)  => (),
        Err(e) => println!("{:?}", e)
    };

    match service.shutdown().map_err(|e| e.to_string()) {
        Ok(_)  => (),
        Err(e) => println!("{:?}", e)
    };

    Ok(())
}

#[test]
fn foo() -> Result<(), String> {

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    rt.spawn(async {
        sleep(Duration::from_secs(100)).await;
    });

    print!("{}", rt.metrics().num_alive_tasks());

    Ok(())
}