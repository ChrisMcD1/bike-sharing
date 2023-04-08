use crate::purchasing_record::Purchase;
use apache_avro::{from_value, AvroSchema, Reader, Writer};
mod purchasing_record;

fn main() {
    println!("Hello, world!");

    let schema = Purchase::get_schema();

    let buffer = Vec::new();

    let mut writer = Writer::new(&schema, buffer);

    let record = Purchase {
        cost: 1.2,
        bike_id: 1,
    };

    writer.append_ser(record).unwrap();

    let second_record = Purchase {
        cost: 1.2,
        bike_id: 2,
    };

    writer.append_ser(second_record).unwrap();

    let third_record = Purchase {
        cost: 1.2,
        bike_id: 4,
    };

    writer.append_ser(third_record).unwrap();

    let encoded = writer.into_inner().unwrap();

    println!("schema: {:?}", schema);
    println!("encoded is {:?} long", encoded.len());

    let reader = Reader::new(&encoded[..]).unwrap();
    for record in reader {
        println!("purchase: {:?}", from_value::<Purchase>(&record.unwrap()))
    }
}
