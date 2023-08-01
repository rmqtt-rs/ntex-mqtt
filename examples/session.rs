#![type_length_limit = "1638773"]

use futures::future::ok;
use ntex::{fn_factory_with_config, fn_service};
use ntex_mqtt::v5::codec::{PublishAckReason};
use ntex_mqtt::{v3, v5, MqttServer};
use ntex_mqtt::v3::QoS;
use ntex_mqtt::v5::codec::{Auth, DisconnectReasonCode};

#[derive(Clone, Debug)]
struct MySession {
    // our custom session information
    client_id: String,
}

#[derive(Debug)]
struct MyServerError;

impl From<()> for MyServerError {
    fn from(_: ()) -> Self {
        MyServerError
    }
}

impl std::convert::TryFrom<MyServerError> for v5::PublishAck {
    type Error = MyServerError;

    fn try_from(err: MyServerError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

async fn handshake_v3<Io>(
    handshake: v3::Handshake<Io>,
) -> Result<v3::HandshakeAck<Io, MySession>, MyServerError> {
    log::info!("new connection: {:?}", handshake);

    let session = MySession { client_id: handshake.packet().client_id.to_string() };

    Ok(handshake.ack(session, false).idle_timeout(90))
}

async fn publish_v3(
    session: v3::Session<MySession>,
    publish: v3::PublishMessage,
) -> Result<(), MyServerError> {
    log::info!(
        "incoming publish ({:?})",
        session.state(),
//        publish.id(),
//        publish.topic()
    );

    // example: only "my-client-id" may publish
    if session.state().client_id == "my-client-id" {
        Ok(())
    } else {
        // with MQTTv3 we can only close the connection
        Err(MyServerError)
    }
}

async fn handshake_v5<Io>(
    handshake: v5::Handshake<Io>,
) -> Result<v5::HandshakeAck<Io, MySession>, MyServerError> {
    log::info!("new connection: {:?}", handshake);

    let session = MySession { client_id: handshake.packet().client_id.to_string() };

    Ok(handshake.ack(session))
}

async fn publish_v5(
    session: v5::Session<MySession>,
    publish: v5::PublishMessage,
) -> Result<v5::PublishResult, MyServerError> {
    log::info!(
        "incoming publish ({:?})",
        session.state(),
//        publish.id(),
//        publish.topic()
    );

    // example: only "my-client-id" may publish
    if session.state().client_id == "my-client-id" {
        Ok(publish.ack())
    } else {
        Ok(publish.ack())
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "session=info,ntex=info,ntex_mqtt=info,basic=info");
    env_logger::init();

    log::info!("Hello");

    ntex::server::Server::build()
        .bind("mqtt", "0.0.0.0:1883", || {
            MqttServer::new()
                .v3(v3::MqttServer::new(handshake_v3).publish(fn_factory_with_config(
                    |session: v3::Session<MySession>| {
                        ok::<_, MyServerError>(fn_service(move |req| {
                            publish_v3(session.clone(), req)
                        }))
                    },
                )).control(fn_factory_with_config(
                    |session: v3::Session<MySession>| {
                        ok::<_, MyServerError>(fn_service(move |req| {
                            control_message_v3(session.clone(), req)
                        }))
                    },
                )))
//                .v5(v5::MqttServer::new(handshake_v5).publish(fn_factory_with_config(
//                    |session: v5::Session<MySession>| {
//                        ok::<_, MyServerError>(fn_service(move |req| {
//                            publish_v5(session.clone(), req)
//                        }))
//                    },
//                )))
        })?
        .workers(16)
        .run()
        .await
}



#[inline]
async fn control_message_v3(
    state: v3::Session<MySession>,
    ctrl_msg: v3::ControlMessage,
) -> Result<v3::ControlResult, MyServerError> {
    log::info!("incoming control message -> {:?}", ctrl_msg);

    let crs = match ctrl_msg {
        v3::ControlMessage::Subscribe(mut subs) => {
            for mut sub in subs.iter_mut() {
                sub.confirm(QoS::ExactlyOnce)
            }
            subs.ack()
        }
        v3::ControlMessage::Unsubscribe(unsubs) => unsubs.ack(),
        v3::ControlMessage::Ping(ping) => ping.ack(),
        v3::ControlMessage::Disconnect(disc) => {
            disc.ack()
        }
        v3::ControlMessage::Closed(m) => {
            m.ack()
        }
    };

    Ok(crs)
}

async fn control_message_v5<E: std::fmt::Debug>(
    state: v5::Session<MySession>,
    ctrl_msg: v5::ControlMessage<E>,
) -> Result<v5::ControlResult, MyServerError> {
    log::debug!("incoming control message -> {:?}", ctrl_msg);

    let crs = match ctrl_msg {
        v5::ControlMessage::Auth(auth) => auth.ack(Auth::default()),
        v5::ControlMessage::Ping(ping) => ping.ack(),
        v5::ControlMessage::Subscribe(subs) => subs.ack(),
        v5::ControlMessage::Unsubscribe(unsubs) => unsubs.ack(),
        v5::ControlMessage::Disconnect(disconnect) => disconnect.ack(),
        v5::ControlMessage::Closed(closed) => closed.ack(),
        v5::ControlMessage::Error(err) => err.ack(DisconnectReasonCode::ServerBusy),
        v5::ControlMessage::ProtocolError(protocol_error) => protocol_error.ack(),
    };

    Ok(crs)
}
