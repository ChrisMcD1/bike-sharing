use apache_avro::{from_value, AvroSchema};
use kafka::{consumer::Consumer, producer::Producer};
use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::async_impl::schema_registry::{post_schema, SrSettings};
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use serde::{Deserialize, Serialize};
use std::{time::Duration, vec};
use tokio::time::sleep;

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
struct Configuration {
    schema_registry_address: String,
    kafka_address: String,
}

const TOPIC: &str = "bike-purchases-aggregator";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("Hello, world!");

    let schema_registry_address =
        std::env::var("SCHEMA_REGISTRY_ADDRESS").expect("Must define $SCHEMA_REGISTRY_ADDRESS");
    let kafka_broker_address =
        std::env::var("KAFKA_BROKER_ADDRESS").expect("Must define $KAFKA_BROKER_ADDRESS");

    let mut kafka_producer = create_producer(&kafka_broker_address).await;

    let mut kafka_consumer = create_consumer(&kafka_broker_address).await;

    let sr_settings = SrSettings::new(schema_registry_address.to_owned());
    let supplied_schema = SuppliedSchema {
        name: None,
        schema_type: SchemaType::Avro,
        schema: PurchaseAggregate::get_schema().canonical_form(),
        references: vec![],
    };
    while let Err(e) = post_schema(
        &sr_settings,
        format!("{}-value", TOPIC.to_owned()),
        supplied_schema.clone(),
    )
    .await
    {
        println!(
            "Failed to connect to schema registry with error: {}. Retrying in 1 second",
            e
        );
        sleep(Duration::from_secs(1)).await;
    }

    let decoder = AvroDecoder::new(sr_settings.clone());

    let encoder = AvroEncoder::new(sr_settings);
    let subject_name_strategy = SubjectNameStrategy::TopicNameStrategy(TOPIC.to_owned(), false);

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
                    .await
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
                )
                .await;
                println!("{:?}", aggregate);
            }
            message_set_count += 1;
            println!("Finished Message set {message_set_count}");
        }
    }
}

async fn send_to_producer(
    producer: &mut Producer,
    encoder: &AvroEncoder<'_>,
    subject_name_strategy: &SubjectNameStrategy,
    aggregate: &PurchaseAggregate,
) {
    let avro_binary = encoder
        .encode_struct(aggregate, subject_name_strategy)
        .await
        .expect("Unable to encode aggregate struct");
    let kafka_record = kafka::producer::Record::from_value(TOPIC, avro_binary);
    producer
        .send(&kafka_record)
        .expect("Unable to send to aggregate producer");
}

async fn create_producer(kafka_broker_address: &str) -> Producer {
    loop {
        match Producer::from_hosts(vec![kafka_broker_address.to_owned()])
            .with_ack_timeout(std::time::Duration::from_secs(1))
            .with_required_acks(kafka::producer::RequiredAcks::One)
            .create()
        {
            Ok(producer) => return producer,
            Err(kafka::Error::NoHostReachable) => {
                println!("Unable to reach host, retrying in 1 second");
                sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("Failed with unexpected error in creating producer {}", e),
        }
    }
}

async fn create_consumer(kafka_broker_address: &str) -> Consumer {
    loop {
        match Consumer::from_hosts(vec![kafka_broker_address.to_owned()])
            .with_topic("bike-purchases".to_owned())
            .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
            .with_fetch_max_wait_time(Duration::from_secs(1))
            .with_fetch_min_bytes(1000)
            .with_fetch_max_bytes_per_partition(100_000)
            .with_retry_max_bytes_limit(1_000_000)
            .create()
        {
            Ok(consumer) => return consumer,
            Err(kafka::Error::Kafka(kafka::error::KafkaCode::UnknownTopicOrPartition)) => {
                println!("Failed to create consumer because of bad topic or partition. Retrying in 1 second");
                sleep(Duration::from_secs(1)).await;
            }
            Err(e) => {
                println!("Failed with unexpected error in creating consumer {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
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
