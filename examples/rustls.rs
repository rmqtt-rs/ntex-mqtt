use std::{fs::File, io::BufReader};

use ntex::pipeline_factory;
use ntex::rt::net::TcpStream;
use ntex::server::rustls::Acceptor;
use ntex_mqtt::{v3, v5, MqttError, MqttServer};
use rustls::ServerConfig;
use rustls_pemfile::certs;
use tokio_rustls::server::TlsStream;

#[derive(Clone)]
struct Session;

#[derive(Debug)]
struct ServerError;

impl From<()> for ServerError {
    fn from(_: ()) -> Self {
        ServerError
    }
}

impl std::convert::TryFrom<ServerError> for v5::PublishAck {
    type Error = ServerError;

    fn try_from(err: ServerError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

impl std::convert::TryFrom<ServerError> for v5::PublishResult {
    type Error = ServerError;
    #[inline]
    fn try_from(e: ServerError) -> Result<Self, Self::Error> {
        Err(e)
    }
}

async fn handshake_v3(
    handshake: v3::Handshake<TlsStream<TcpStream>>,
) -> Result<v3::HandshakeAck<TlsStream<TcpStream>, Session>, ServerError> {
    log::info!("new connection: {:?}", handshake);
    Ok(handshake.ack(Session, false))
}

//pub async fn publish(state: v3::Session<SessionState>, pub_msg: v3::PublishMessage) -> Result<(), MqttError> {
async fn publish_v3(publish: v3::PublishMessage) -> Result<(), ServerError> {
    log::info!("incoming publish: {:?}", publish);
    Ok(())
}

async fn handshake_v5(
    handshake: v5::Handshake<TlsStream<TcpStream>>,
) -> Result<v5::HandshakeAck<TlsStream<TcpStream>, Session>, ServerError> {
    log::info!("new connection: {:?}", handshake);
    Ok(handshake.ack(Session))
}

async fn publish_v5(publish: v5::PublishMessage) -> Result<v5::PublishResult, ServerError> {
    log::info!("incoming publish: {:?}", publish);
    Ok(publish.ack())
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,basic=trace");
    env_logger::init();

    // let mut tls_config = ServerConfig::new(NoClientAuth::new());

    // // create self-signed certificates using:
    // //   openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:4096 -keyout examples/key8.pem -out examples/cert.pem -days 365 -keyform PEM
    // //   openssl rsa -in examples/key8.pem -out examples/key.pem
    // let cert_file = &mut BufReader::new(File::open("./examples/cert.pem").unwrap());
    // let key_file = &mut BufReader::new(File::open("./examples/key.pem").unwrap());

    // let cert_chain = certs(cert_file).unwrap();
    // let mut keys = rsa_private_keys(key_file).unwrap();
    // tls_config.set_single_cert(cert_chain, keys.remove(0)).unwrap();
    let cert_file = &mut BufReader::new(File::open("./examples/cert.pem").unwrap());
    let key_file = &mut BufReader::new(File::open("./examples/key.pem").unwrap());

    let key = rustls_pemfile::private_key(key_file).unwrap().unwrap();
    let cert_chain = certs(cert_file).map(|r| r.unwrap()).collect();

    let tls_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .unwrap();
    let tls_acceptor = Acceptor::new(tls_config);

    ntex::server::Server::build()
        .bind("mqtt", "0.0.0.0:8883", move || {
            pipeline_factory(tls_acceptor.clone())
                .map_err(|_err| MqttError::Service(ServerError {})) //ntex_mqtt::MqttError::Service(MqttError::from(e))
                .and_then(
                    MqttServer::new()
                        .v3(v3::MqttServer::new(handshake_v3).publish(publish_v3))
                        .v5(v5::MqttServer::new(handshake_v5).publish(publish_v5)),
                )
        })?
        .workers(1)
        .run()
        .await
}
