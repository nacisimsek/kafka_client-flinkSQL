import csv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

def read_config(config_file="client.properties"):
    config = {}
    with open(config_file, "r") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, val = line.split("=", 1)
            config[key.strip()] = val.strip()
    return config

# Key and value Avro schemas
KEY_SCHEMA_STR = """
{
  "type": "record",
  "name": "NetflixKey",
  "namespace": "com.example",
  "fields": [
    {"name": "user_id", "type": "string"}
  ]
}
"""

VALUE_SCHEMA_STR = """
{
  "type": "record",
  "name": "NetflixEvent",
  "namespace": "com.example",
  "fields": [
    {"name": "datetime",      "type": "string"},
    {"name": "duration",      "type": "float"},
    {"name": "title",         "type": "string"},
    {"name": "genres",        "type": "string"},
    {"name": "release_date",  "type": "string"},
    {"name": "movie_id",      "type": "string"},
    {"name": "user_id",       "type": "string"}
  ]
}
"""

def produce_data(topic="netflix-uk-views",
                 config_file="client.properties",
                 csv_file="netflix_uk_dataset.csv"):

    # Load base config
    base_conf = read_config(config_file)

    # Separate Kafka & Schema Registry configs
    kafka_conf = {
        "bootstrap.servers": base_conf["bootstrap.servers"],
        "security.protocol": base_conf["security.protocol"],
        "sasl.mechanisms":   base_conf["sasl.mechanisms"],
        "sasl.username":     base_conf["sasl.username"],
        "sasl.password":     base_conf["sasl.password"],
    }

    sr_conf = {
        "url": base_conf["schema.registry.url"],
        "basic.auth.user.info": base_conf["basic.auth.user.info"]
    }

    # Create Schema Registry Client
    schema_registry_client = SchemaRegistryClient(sr_conf)

    # Set up Avro serializers
    key_avro_serializer = AvroSerializer(
        schema_registry_client,
        KEY_SCHEMA_STR,
        to_dict=lambda d, _: d  # 'to_dict' just returns the dict as-is
    )
    value_avro_serializer = AvroSerializer(
        schema_registry_client,
        VALUE_SCHEMA_STR,
        to_dict=lambda d, _: d
    )

    # Build the producer config with serializers
    producer_conf = {
        **kafka_conf,
        "key.serializer": key_avro_serializer,
        "value.serializer": value_avro_serializer
    }

    producer = SerializingProducer(producer_conf)

    def on_delivery(err, msg):
        if err:
            print(f"Delivery failed for key {msg.key()}: {err}")
        else:
            print(f"Delivered record with key {msg.key()} to {msg.topic()} "
                  f"[{msg.partition()}]@{msg.offset()}")

    count = 0
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            # Remove blank index col if present
            row.pop("", None)

            # Convert duration to float
            try:
                row["duration"] = float(row["duration"])
            except ValueError:
                row["duration"] = 0.0

            key_dict = {"user_id": row["user_id"]}
            value_dict = {
                "datetime":     row["datetime"],
                "duration":     row["duration"],
                "title":        row["title"],
                "genres":       row["genres"],
                "release_date": row["release_date"],
                "movie_id":     row["movie_id"],
                "user_id":      row["user_id"],
            }

            producer.produce(
                topic=topic,
                key=key_dict,
                value=value_dict,
                on_delivery=on_delivery
            )
            producer.poll(0)  # avoid queue-full errors

            count += 1
            # Optionally flush every 10k records
            if i % 10000 == 0:
                producer.flush()
                print(f"Flushed after {i} records...")

    producer.flush()  # final flush
    print(f"Done! Produced {count} records to '{topic}' in Avro format.")

def main():
    produce_data()

if __name__ == "__main__":
    main()