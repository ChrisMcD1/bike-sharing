mod produces;
use crate::produces::User;
use actix_web::{middleware::Logger, post, web::Json, App, HttpServer, Responder};
use env_logger::Env;
use kafka::producer::Producer;
use produces::USER_TOPIC;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::Deserialize;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Hello, world!");
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let bind_ip = std::env::var("BIND_IP").expect("Must define $BIND_IP");
    let bind_port: u16 = std::env::var("BIND_PORT")
        .expect("Must define $BIND_PORT")
        .parse()
        .expect("Got binding port, but failed to parse to u16");

    HttpServer::new(move || App::new().service(create).wrap(Logger::default()))
        .bind((bind_ip, bind_port))?
        .run()
        .await
}

#[derive(Debug, Deserialize)]
pub struct UserCreationRequest {
    pub username: String,
    pub first_name: String,
    pub last_name: String,
    pub password: String,
}

#[post("/create")]
pub async fn create(request: Json<UserCreationRequest>) -> impl Responder {
    let mut producer = UsersProducer::new();

    let user = User {
        id: 1,
        username: request.username.clone(),
        first_name: request.first_name.clone(),
        last_name: request.last_name.clone(),
        password: request.password.clone(),
    };
    producer.send_record(user);
    "got it bossman"
}

struct UsersProducer {
    kafka_producer: Producer,
    subject_name_strategy: SubjectNameStrategy,
    encoder: AvroEncoder,
}

impl UsersProducer {
    pub fn new() -> Self {
        let kafka_broker_address =
            std::env::var("KAFKA_BROKER_ADDRESS").expect("Must define $KAFKA_BROKER_ADDRESS");
        let kafka_producer = Producer::from_hosts(vec![kafka_broker_address.to_owned()])
            .with_ack_timeout(std::time::Duration::from_secs(1))
            .with_required_acks(kafka::producer::RequiredAcks::One)
            .create()
            .expect("Unable to make kafka producer");

        let schema_registry_address =
            std::env::var("SCHEMA_REGISTRY_ADDRESS").expect("Must define $SCHEMA_REGISTRY_ADDRESS");
        let sr_settings = SrSettings::new(schema_registry_address.to_owned());
        let encoder = AvroEncoder::new(sr_settings);

        let subject_name_strategy =
            SubjectNameStrategy::TopicNameStrategy(USER_TOPIC.to_owned(), false);

        Self {
            kafka_producer,
            subject_name_strategy,
            encoder,
        }
    }
    pub fn send_record(&mut self, record: User) {
        let avro_binary = self
            .encoder
            .encode_struct(record, &self.subject_name_strategy)
            .expect("Unable to encode user");
        let kafka_record = kafka::producer::Record::from_value(USER_TOPIC, avro_binary);
        self.kafka_producer
            .send(&kafka_record)
            .expect("Unable to send to user stream");
    }
}
