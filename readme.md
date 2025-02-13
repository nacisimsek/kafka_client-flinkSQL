# Kafka Producer and FlinkSQL

In this hands-on practise, I will be implementing a **Kafka Producer** in python, which will use a local csv dataset, and generate each row of this csv dataset as an **AVRO** serialized events to the Kafka topic in **[Confluent Cloud](https://confluent.cloud/home)**.

Then based on this topic, several analytics queries will be performed by using Confluent Coud for Apache Flink service.

## 1. Create an account on Confluent Cloud

Follow the steps given [here](https://docs.confluent.io/cloud/current/get-started/index.html "Quick Start for Confluent Cloud") to sign up for Confluent Cloud, and create Kafka Cluster.

Make sure you setup:

* Security for your Kafka Cluster ([API Keys](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/manage-api-keys.html "Manage API Keys in Confluent Cloud") to connect)
* If applicable, also setup API keys to your **Schema Registry**.

## 2. Create client.properties For Configurations

This file will include important configuration items which will be used to connect the Kafka Cluster and Schema Registry to perform event producing.

Here are the essential configuration parameters:

| Configuration Item                                                               | Description                                              |
| -------------------------------------------------------------------------------- | -------------------------------------------------------- |
| **`bootstrap.servers`**                                                  | Specifies the Kafka broker addresses.                    |
| **`security.protocol`**                                                  | Defines the protocol used to communicate with brokers.   |
| **`sasl.mechanisms`**                                                    | Specifies the SASL mechanism used for authentication.    |
| **`sasl.username`** & **`sasl.password`**                        | Credentials for SASL authentication.                     |
| **`schema.registry.url`**                                                | URL of the Schema Registry service.                      |
| **`basic.auth.credentials.source`** & **`basic.auth.user.info`** | Credentials for authenticating with the Schema Registry. |

You can fill these parameter values based on your setup. If you setup production pipelines, you may consider [other ways](https://docs.confluent.io/cloud/current/security/authenticate/overview.html) to pass such sensitive parameters to your cluster.

## 3. Analyze the Dataset

Before processing the dataset and push it to a Kafka topic, make sure to perform basic check on the data, which is listed as follows:

* **Data Profile:** Analyze data for nulls, types, and inconsistencies.
* **Schema Design:** Define Avro schema, especially for date/time fields, precisions, etc...
* **Clean & Transform:** Handle missing/invalid data; normalize timezones.
* **Verify Kafka:** Check data in Kafka topic for correctness.

## 4. Create a Producer and Execute

There are several ways to create a producer to produce events to your Kafka Cluster. See [here](https://docs.confluent.io/cloud/current/get-started/index.html#step-3-create-a-sample-producer "Create a sample producer") for more information.

However, for this project, we will be building our own [client application](https://developer.confluent.io/tutorials/creating-first-apache-kafka-producer-application/confluent.html "How to build your first Apache KafkaProducer application") (script) to produce the **csv** data from our local to the Kafka Cluster we created.

There are several languages you can choose to create a producer, I will be using **python** in this example.

The python script which works as a producer is called `kafka_python_producer.py`. See the code comments for more details on the implementation.

To run the python script as a Producer Client, perform the steps below:

1. Create a virtual environment `env` for your python script execution:

```powershell
python3 -m venv env
```

2. Activate the generated virtual environment:

```powershell
source env/bin/activate
```

3. Install the base **confluent-kafka** package **plus** all dependencies required for Avro serialization/deserialization.

```powershell
pip install "confluent-kafka[avro]"
```

4. Finally, execute the script to perform message production to the Kafka Cluster:

```powershell
python3 kafka_python_producer.py
```

## 5. Check Confluent Cloud UI to Verify the Messages on Topic

Click `Home` > `Environment (Select the environment)` > `Cluster` > `Topics` > `Messages` to see if the messages are all pushed to the related Kafka Topic:

![ScreenShot_20250125_155820@2x](https://github.com/user-attachments/assets/4c0c4a28-ae5c-48c2-b58b-feb890be5799)

It is also important to compare the data in the raw dataset with the ones published into the topic, to make sure all the required data is there in the topic.

## 6. Perform Analytical Queries on Kafka Topic

We can now perform analytical queries on our Kafka topic by using **Flink streaming**. By executing FlinkSQL queries on Kafka topic, Flink will perform. real-time aggregations and calculations to generate the required data.

If we want this data to be materialized, we can use various Flink connectors to do so. But this is out of scope of this project. We will only be observing the results of the Flink State through the UI.

To open the workspace for executing FlinkSQL queries, click to **Query with Flink** button on the topic page. or navigate to `Stream processing` > `Create workspace`. If you want to open an existing workspace, simply go to `Home` > `Stream processing` and select your existing workspace.

1. Calculate the **average watch duration** for **each movie title** across all users.
   Below SQL is used to perform the query:

   ```sql
   SELECT
     title,
     AVG(duration) AS avg_watch_duration
   FROM `default`.`kafka_naci`.`netflix-uk-views`
   GROUP BY title;
   ```
2. Analyze **daily** engagement patterns for **each movie title**. Calculate **daily** **view counts** and **total watch time** for each title to track how user interest fluctuates day by day.
   Below SQL is used to perform the query:

   ```sql
   SELECT
       window_start,
       window_end,
       title,
       COUNT(*) AS daily_view_count,
       SUM(duration) AS daily_total_watch_time
   FROM TABLE (
       TUMBLE(TABLE `default`.`kafka_naci`.`netflix-uk-views`, DESCRIPTOR(datetime), INTERVAL '1' DAY)
   )
   GROUP BY window_start, window_end, title;
   ```
