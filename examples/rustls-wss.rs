use std::{fs::File, io, io::BufReader, marker, time};

use ntex::rt::net::TcpStream;
use ntex::server::rustls::Acceptor;
use ntex::{fn_factory_with_config, fn_service, pipeline_factory, Service, ServiceFactory};
use ntex_mqtt::{v3, v5, MqttError, MqttServer, TopicError};
use rustls::internal::pemfile::{certs, rsa_private_keys};
use rustls::{NoClientAuth, ServerConfig};
use tokio_rustls::server::TlsStream;

use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Error as WsError;

#[derive(Clone)]
struct Session;

#[derive(Debug)]
pub struct ServerError;

impl From<()> for ServerError {
    fn from(_: ()) -> Self {
        ServerError
    }
}

impl From<io::Error> for ServerError {
    fn from(e: io::Error) -> Self {
        ServerError
    }
}

impl std::convert::TryFrom<ServerError> for MqttError<ServerError> {
    type Error = ServerError;
    fn try_from(err: ServerError) -> Result<Self, Self::Error> {
        Err(err)
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
    fn try_from(e: ServerError) -> Result<Self, Self::Error> {
        Err(e)
    }
}

impl From<SendPacketError> for ServerError {
    #[inline]
    fn from(e: SendPacketError) -> Self {
        ServerError
    }
}

impl From<TopicError> for ServerError {
    #[inline]
    fn from(e: TopicError) -> Self {
        ServerError
    }
}

async fn handshake_v3(
    handshake: v3::Handshake<WsStream<TlsStream<TcpStream>>>,
) -> Result<v3::HandshakeAck<WsStream<TlsStream<TcpStream>>, Session>, ServerError> {
    log::info!("new connection: {:?}", handshake);
    Ok(handshake.ack(Session, false))
}

async fn publish_v3(publish: v3::PublishMessage) -> Result<(), ServerError> {
    log::info!("incoming publish: {:?}", publish);
    Ok(())
}

async fn handshake_v5(
    handshake: v5::Handshake<WsStream<TlsStream<TcpStream>>>,
) -> Result<v5::HandshakeAck<WsStream<TlsStream<TcpStream>>, Session>, ServerError> {
    log::info!("new connection: {:?}", handshake);
    Ok(handshake.ack(Session))
}

async fn publish_v5(publish: v5::PublishMessage) -> Result<v5::PublishResult, ServerError> {
    log::info!("incoming publish: {:?}", publish);
    Ok(publish.ack())
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,rustls_wss=trace");
    env_logger::init();

    let mut tls_config = ServerConfig::new(NoClientAuth::new());

    // create self-signed certificates using:
    //   openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:4096 -keyout examples/key8.pem -out examples/cert.pem -days 365 -keyform PEM
    //   openssl rsa -in examples/key8.pem -out examples/key.pem
    let cert_file = &mut BufReader::new(File::open("./examples/cert.pem").unwrap());
    let key_file = &mut BufReader::new(File::open("./examples/key.pem").unwrap());

    let cert_chain = certs(cert_file).unwrap();
    let mut keys = rsa_private_keys(key_file).unwrap();
    tls_config.set_single_cert(cert_chain, keys.remove(0)).unwrap();

    let tls_acceptor = Acceptor::new(tls_config);

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:8883", move || {
            pipeline_factory(tls_acceptor.clone())
                .map_err(|_err| MqttError::Service(ServerError {}))
                .and_then(WSServer::new())
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

use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::testing::Io;
use ntex::util::Ready;
use ntex_mqtt::error::SendPacketError;
use std::error::Error;
use std::task::{Context, Poll};

pub struct WSServer<T> {
    timeout: time::Duration,
    io: marker::PhantomData<T>,
}

impl<T: AsyncRead + AsyncWrite> WSServer<T> {
    /// Create rustls based `Acceptor` service factory
    pub fn new() -> Self {
        WSServer { timeout: time::Duration::from_secs(5), io: marker::PhantomData }
    }

    /// Set handshake timeout in milliseconds
    ///
    /// Default is set to 5 seconds.
    pub fn timeout(mut self, time: u64) -> Self {
        self.timeout = time::Duration::from_millis(time);
        self
    }
}

impl<T> Clone for WSServer<T> {
    fn clone(&self) -> Self {
        Self { timeout: self.timeout, io: marker::PhantomData }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + 'static> ServiceFactory for WSServer<T> {
    type Request = T;
    type Response = WsStream<T>;
    type Error = MqttError<ServerError>;
    type Service = WSService<T>;

    type Config = ();
    type InitError = ();
    type Future = Ready<Self::Service, Self::InitError>;

    fn new_service(&self, _: ()) -> Self::Future {
        Ready::Ok(WSService { timeout: self.timeout, io: marker::PhantomData })
    }
}

/// RusTLS based `Acceptor` service
pub struct WSService<T> {
    io: marker::PhantomData<T>,
    timeout: time::Duration,
}

impl<T: AsyncRead + AsyncWrite + Unpin + 'static> Service for WSService<T> {
    type Request = T;
    type Response = WsStream<T>;
    type Error = MqttError<ServerError>;
    type Future = WSServiceFut<T>;
    //type Future = Box<dyn Future<Output = Result<WsStream<T>, Self::Error>>>;
    //type Future = Pin<Box<dyn Future<Output = Result<WsStream<T>, Self::Error>> >>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&self, req: Self::Request) -> Self::Future {
        WSServiceFut {
            fut: accept_hdr_async(req, on_handshake).boxed_local(),
            delay: if self.timeout == ZERO { None } else { Some(sleep(self.timeout)) },
        }
    }
}

pub(self) const ZERO: std::time::Duration = std::time::Duration::from_millis(0);
use ntex::rt::time::{sleep, Sleep};

type WebSocketStreamType<T> = Pin<Box<dyn Future<Output = Result<WebSocketStream<T>, WsError>>>>;

pin_project_lite::pin_project! {
    pub struct WSServiceFut<T>
    where
        T: AsyncRead,
        T: AsyncWrite,
        T: Unpin,
    {
        fut: WebSocketStreamType<T>,
        #[pin]
        delay: Option<Sleep>,
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Future for WSServiceFut<T> {
    type Output = Result<WsStream<T>, MqttError<ServerError>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if let Some(delay) = this.delay.as_pin_mut() {
            match delay.poll(cx) {
                Poll::Pending => (),
                Poll::Ready(_) => return Poll::Ready(Err(MqttError::HandshakeTimeout)),
            }
        }
        match Pin::new(&mut this.fut).poll(cx) {
            Poll::Ready(Ok(io)) => Poll::Ready(Ok(WsStream::new(io))),
            Poll::Ready(Err(e)) => Poll::Ready(Err(MqttError::Service(ServerError))),
            Poll::Pending => Poll::Pending,
        }
    }
}

use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};

fn on_handshake(req: &Request, mut response: Response) -> std::result::Result<Response, ErrorResponse> {
    const PROTOCOL_ERROR: &str = "No \"Sec-WebSocket-Protocol: mqtt\" in client request";
    let sec_ws_protocol = req
        .headers()
        .get("Sec-WebSocket-Protocol")
        .ok_or_else(|| ErrorResponse::new(Some(PROTOCOL_ERROR.into())))?;
    if sec_ws_protocol != "mqtt" {
        return Err(ErrorResponse::new(Some(PROTOCOL_ERROR.into())));
    }
    response.headers_mut().append("Sec-WebSocket-Protocol", HeaderValue::from_static("mqtt"));
    Ok(response)
}

use std::future::Future;
use std::io::{ErrorKind, Write};
use std::pin::Pin;

use futures::{ready, FutureExt, Sink, SinkExt, Stream, StreamExt};
use ntex::http::header::HeaderValue;
use tokio_tungstenite::tungstenite::Error as WSError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub struct WsStream<S> {
    inner: WebSocketStream<S>,
}

impl<S> WsStream<S> {
    pub fn new(inner: WebSocketStream<S>) -> Self {
        Self { inner }
    }
}

impl<S> AsyncRead for WsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ntex::codec::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
            Some(Ok(msg)) => {
                let data = msg.into_data();
                buf.put_slice(data.as_slice());
                Poll::Ready(Ok(()))
            }
            Some(Err(e)) => {
                log::warn!("{:?}", e);
                Poll::Ready(Err(to_io_error(e)))
            }
            None => Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))),
        }
    }
}

impl<S> AsyncWrite for WsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if let Err(e) = Pin::new(&mut self.inner).start_send(Message::Binary(buf.to_vec())) {
            return Poll::Ready(Err(to_io_error(e)));
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if let Err(e) = ready!(Pin::new(&mut self.inner).poll_flush(cx)) {
            return Poll::Ready(Err(to_io_error(e)));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if let Err(e) = ready!(Pin::new(&mut self.inner).poll_close(cx)) {
            return Poll::Ready(Err(to_io_error(e)));
        }
        Poll::Ready(Ok(()))
    }
}

fn to_io_error(e: WSError) -> io::Error {
    match e {
        WSError::ConnectionClosed => io::Error::from(ErrorKind::ConnectionAborted),
        WSError::AlreadyClosed => io::Error::from(ErrorKind::NotConnected),
        WSError::Io(io_e) => io::Error::from(io_e),
        _ => io::Error::new(ErrorKind::Other, e.to_string()),
    }
}
