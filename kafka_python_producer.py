import csv
from datetime import datetime, date

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

# -------------------------------------------------------------------
# Avro Schemas
# -------------------------------------------------------------------
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
    {
      "name": "datetime",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "duration",
      "type": "float"
    },
    {
      "name": "title",
      "type": "string"
    },
    {
      "name": "genres",
      "type": "string"
    },
    {
      "name": "release_date",
      "type": [
        "null",
        {
          "type": "int",
          "logicalType": "date"
        }
      ],
      "default": null
    },
    {
      "name": "movie_id",
      "type": "string"
    },
    {
      "name": "user_id",
      "type": "string"
    }
  ]
}
"""

# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------
def parse_datetime_string(dt_str):
    """
    Convert "YYYY-MM-DD HH:MM:SS" to epoch millis (int).
    Assuming all datetimes in the CSV are valid.
    """
    dt_format = "%Y-%m-%d %H:%M:%S"
    dt_obj = datetime.strptime(dt_str, dt_format)
    return int(dt_obj.timestamp() * 1000)  # seconds -> ms

def parse_release_date(rd_str):
    """
    Convert "YYYY-MM-DD" to days since epoch (int).
    If invalid or "NOT AVAILABLE", return None (so Avro stores null).
    """
    rd_str_clean = rd_str.strip().upper()
    if rd_str_clean == "NOT AVAILABLE" or not rd_str_clean:
        return None
    try:
        d = datetime.strptime(rd_str, "%Y-%m-%d").date()
        return (d - date(1970, 1, 1)).days
    except ValueError:
        return None

# -------------------------------------------------------------------
# Main production logic
# -------------------------------------------------------------------
def produce_data(topic="netflix-uk-views",
                 config_file="client.properties",
                 csv_file="netflix_uk_dataset.csv"):

    # 1) Read base configs
    base_conf = read_config(config_file)

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

    # 2) Initialize schema registry client
    schema_registry_client = SchemaRegistryClient(sr_conf)

    # 3) Avro serializers
    key_avro_serializer = AvroSerializer(
        schema_registry_client,
        KEY_SCHEMA_STR,
        to_dict=lambda d, _: d
    )
    value_avro_serializer = AvroSerializer(
        schema_registry_client,
        VALUE_SCHEMA_STR,
        to_dict=lambda d, _: d
    )

    # 4) Producer config
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

    # 5) Read CSV and produce messages
    count = 0
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            # Remove blank index col if present
            row.pop("", None)

            # Parse numeric duration
            try:
                row["duration"] = float(row["duration"])
            except ValueError:
                row["duration"] = 0.0

            # Convert datetime -> epoch millis
            dt_millis = parse_datetime_string(row["datetime"])  
            # We assume the CSV always has a valid datetime

            # Convert release_date -> int days since epoch or None
            rd_days = parse_release_date(row["release_date"])

            # Construct key & value
            key_dict = {"user_id": row["user_id"]}
            value_dict = {
                "datetime":     dt_millis,       # mandatory, always a long
                "duration":     row["duration"],
                "title":        row["title"],
                "genres":       row["genres"],
                "release_date": rd_days,         # may be None -> Avro null
                "movie_id":     row["movie_id"],
                "user_id":      row["user_id"],
            }

            producer.produce(
                topic=topic,
                key=key_dict,
                value=value_dict,
                on_delivery=on_delivery
            )
            producer.poll(0)
            count += 1

            if i % 10000 == 0:
                producer.flush()
                print(f"Flushed after {i} records...")

    producer.flush()
    print(f"Done! Produced {count} records to '{topic}' in Avro format.")

def main():
    produce_data()

if __name__ == "__main__":
    main()