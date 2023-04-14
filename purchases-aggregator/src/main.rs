use apache_avro::{from_value, AvroSchema};
use kafka::{consumer::Consumer, producer::Producer};
use schema_registry_converter::blocking::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::{Deserialize, Serialize};
use std::{time::Duration, vec};

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
struct Configuration {
    schema_registry_address: String,
    kafka_address: String,
}

const TOPIC: &str = "bike-purchases-aggregator";

fn main() {
    println!("Hello, world!");

    let config: Configuration =
        confy::load("purchasing-aggregator", None).expect("Failed to load confy configuration");

    let mut kafka_consumer = Consumer::from_hosts(vec![config.kafka_address.to_owned()])
        .with_topic("bike-purchases".to_owned())
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .with_fetch_max_wait_time(Duration::from_secs(1))
        .with_fetch_min_bytes(1000)
        .with_fetch_max_bytes_per_partition(100_000)
        .with_retry_max_bytes_limit(1_000_000)
        .create()
        .expect("Unable to make kafka consumer");

    let mut kafka_producer = Producer::from_hosts(vec![config.kafka_address.to_owned()])
        .with_ack_timeout(std::time::Duration::from_secs(1))
        .with_required_acks(kafka::producer::RequiredAcks::One)
        .create()
        .expect("Unable to make kafka producer");

    let sr_settings = SrSettings::new(config.schema_registry_address.to_owned());
    let decoder = AvroDecoder::new(sr_settings.clone());

    let encoder = AvroEncoder::new(sr_settings);
    let subject_name_strategy = SubjectNameStrategy::TopicNameStrategy(TOPIC.to_owned(), false);

    let purchase_schema = Purchase::get_schema();
    let purchase_cost_schema = PurchaseCostOnly::get_schema();

    let mut bike_count = 0;
    let mut total_cost = 0f64;
    let mut message_set_count = 0;

    loop {
        for message_set in kafka_consumer
            .poll()
            .expect("Failed to poll consumer")
            .iter()
        {
            for message in message_set.messages() {
                let schema_value = decoder
                    .decode(Some(message.value))
                    .expect("Failed to decode message")
                    .value;
                let purchase = from_value::<PurchaseCostOnly>(
                    &schema_value
                        .resolve(&purchase_cost_schema)
                        .expect("Failed to resolve to schema"),
                )
                .expect("Failed to get struct from value");
                bike_count += 1;
                total_cost += purchase.cost;
                let aggregate = PurchaseAggregate {
                    bike_count,
                    total_cost,
                };
                send_to_producer(
                    &mut kafka_producer,
                    &encoder,
                    &subject_name_strategy,
                    &aggregate,
                );
                println!("{:?}", aggregate);
            }
            message_set_count += 1;
            println!("Finished Message set {message_set_count}");
        }
    }
}

fn send_to_producer(
    producer: &mut Producer,
    encoder: &AvroEncoder,
    subject_name_strategy: &SubjectNameStrategy,
    aggregate: &PurchaseAggregate,
) {
    let avro_binary = encoder
        .encode_struct(aggregate, subject_name_strategy)
        .expect("Unable to encode aggregate struct");
    let kafka_record = kafka::producer::Record::from_value(TOPIC, avro_binary);
    producer
        .send(&kafka_record)
        .expect("Unable to send to aggregate producer");
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct Purchase {
    pub bike_id: u32,
    pub cost: f64,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct PurchaseCostOnly {
    pub cost: f64,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct PurchaseAggregate {
    pub bike_count: u32,
    pub total_cost: f64,
}
