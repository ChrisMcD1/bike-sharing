use std::io::Cursor;

use apache_avro::{from_avro_datum, from_value, AvroSchema};
use kafka::consumer::Consumer;
use serde::{Deserialize, Serialize};

fn main() {
    println!("Hello, world!");

    let mut kafka_consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("bike-purchases".to_owned())
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .create()
        .expect("Unable to make kafka consumer");

    let purchase_schema = Purchase::get_schema();

    loop {
        for message_set in kafka_consumer.poll().unwrap().iter() {
            for message in message_set.messages() {
                let mut message_clone = Cursor::new(message.value.to_owned());
                let purchase = from_value::<Purchase>(
                    &from_avro_datum(&purchase_schema, &mut message_clone, None).unwrap(),
                )
                .unwrap();
                println!("{:?}", purchase);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct Purchase {
    pub bike_id: u32,
    pub cost: f64,
}
