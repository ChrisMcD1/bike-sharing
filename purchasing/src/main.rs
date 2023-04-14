mod produces;
mod purchase_request;

use crate::produces::Purchase;
use actix_web::{
    get,
    middleware::Logger,
    post,
    web::{self, Json},
    App, HttpServer, Responder,
};
use env_logger::Env;
use kafka::producer::Producer;
use purchase_request::PurchaseRequest;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::{Deserialize, Serialize};

const TOPIC: &str = "bike-purchases";

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Hello, world!");
    let config: Configuration =
        confy::load("purchasing", None).expect("Unable to get confy configuration");
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let config_clone = config.clone();
    HttpServer::new(move || {
        App::new()
            .service(hello_world)
            .service(purchase)
            .wrap(Logger::default())
            .app_data(web::Data::new(config_clone.to_owned()))
    })
    .bind((config.purchasing_ip, config.purchasing_port))?
    .run()
    .await
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
struct Configuration {
    purchasing_ip: String,
    purchasing_port: u16,
    kafka_host: String,
    schema_registry_address: String,
}

#[get("/hello")]
async fn hello_world() -> impl Responder {
    "Hi"
}

#[post("/purchase")]
async fn purchase(
    record: Json<PurchaseRequest>,
    config: web::Data<Configuration>,
) -> impl Responder {
    let bike_id = 1;
    let purchase = Purchase {
        cost: record.cost,
        bike_id,
    };

    let mut producer = PurchasesProducer::new(&config);
    producer.send_record(purchase);
    "got it bossman"
}

struct PurchasesProducer {
    kafka_producer: Producer,
    subject_name_strategy: SubjectNameStrategy,
    encoder: AvroEncoder,
}

impl PurchasesProducer {
    pub fn new(config: &Configuration) -> Self {
        let kafka_producer = Producer::from_hosts(vec![config.kafka_host.to_owned()])
            .with_ack_timeout(std::time::Duration::from_secs(1))
            .with_required_acks(kafka::producer::RequiredAcks::One)
            .create()
            .expect("Unable to make kafka producer");

        let sr_settings = SrSettings::new(config.schema_registry_address.to_owned());
        let encoder = AvroEncoder::new(sr_settings);
        let subject_name_strategy = SubjectNameStrategy::TopicNameStrategy(TOPIC.to_owned(), false);

        Self {
            kafka_producer,
            subject_name_strategy,
            encoder,
        }
    }
    pub fn send_record(&mut self, record: Purchase) {
        let avro_binary = self
            .encoder
            .encode_struct(record, &self.subject_name_strategy)
            .expect("Unable to encode aggregate struct");
        let kafka_record = kafka::producer::Record::from_value(TOPIC, avro_binary);
        self.kafka_producer
            .send(&kafka_record)
            .expect("Unable to send to aggregate producer");
    }
}
