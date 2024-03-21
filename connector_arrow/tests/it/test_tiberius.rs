use std::sync::Arc;

use connector_arrow::tiberius::TiberiusConnection;
use tiberius::{AuthMethod, Client, Config};
use tokio::{net::TcpStream, runtime};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

fn init() -> TiberiusConnection<Compat<TcpStream>> {
    let _ = env_logger::builder().is_test(true).try_init();

    let rt = Arc::new(
        runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    let url = std::env::var("TIBERIUS_URL").unwrap();
    let url = url::Url::parse(&url).unwrap();

    let mut config = Config::new();
    config.host(url.host().unwrap());
    config.port(url.port().unwrap());
    config.authentication(AuthMethod::sql_server(
        url.username(),
        url.password().unwrap(),
    ));

    let addr = (url.host_str().unwrap(), url.port().unwrap());
    let tcp = rt.block_on(TcpStream::connect(addr)).unwrap();
    tcp.set_nodelay(true).unwrap();

    let client = Client::connect(config, tcp.compat_write());
    let client = rt.block_on(client).unwrap();

    TiberiusConnection::new(rt, client)
}

#[test]
fn query_01() {
    let mut conn = init();
    super::tests::query_01(&mut conn);
}
