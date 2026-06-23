# Real-Time Stock Market Data Pipeline

This is an end-to-end streaming data pipeline I built to pull live stock prices and process them in real-time. Everything is containerized and runs autonomously. I built this to get hands-on experience with core Data Engineering infrastructure like Kafka and Spark Streaming, and to figure out how to wire them together from scratch.

## The Architecture

* **Ingestion:** A custom Python producer pulls live data from the Yahoo Finance API every 5 seconds and pushes the JSON payloads to Kafka.
* **Message Broker:** Apache Kafka (managed by Zookeeper) catches and holds the live data stream.
* **Stream Processing:** A PySpark Structured Streaming application subscribes to the Kafka topic, unpacks the raw bytes, and processes the micro-batches.
* **Storage:** The cleaned data stream is continuously written to a PostgreSQL database via a JDBC driver for permanent storage.
* **Infrastructure:** The entire architecture (including the custom Python environments) is containerized using Docker Compose.

## Tech Stack
* Python, SQL
* Apache Spark (Structured Streaming), Apache Kafka
* PostgreSQL
* Docker, Docker Compose

## ⚙️ How to Run It

Since I containerized the entire environment, you can launch the pipeline from scratch with one command.

1. Clone the repo and boot the infrastructure:
   ```bash
   git clone [https://github.com/yourusername/realtime-stock-pipeline.git](https://github.com/yourusername/realtime-stock-pipeline.git)
   cd realtime-stock-pipeline
   docker compose up -d --build

## 🐛 Challenges & Bugs Overcome
Building this wasn't just writing code; the hardest part was managing the infrastructure. Here are a few of the main hurdles I had to fix to get this running:

* **The 4GB Memory Limit:** I built this entire pipeline inside a cloud environment with only 4GB of RAM. When Spark booted up, it would aggressively consume memory, causing the system to quietly assassinate the Kafka container in the background (resulting in continuous KafkaTimeoutErrors). I fixed this by restricting Spark in the session config (spark.driver.memory = "1g") so all three heavy Java applications could run side-by-side.

* **The Spark/Scala Version Mismatch:** I hit a massive java.lang.NoSuchMethodError when Spark tried to read the Kafka stream. It turned out the newest version of PySpark was clashing with the underlying Scala 2.12 Kafka connector I was using. I resolved it by hardcoding PySpark 3.5.0 into my Dockerfile to perfectly align the engine with the connector.

* **The Java 21 Collision:** Spark relies on Hadoop for file management, which crashed instantly on boot because the default cloud environment updated to Java 21 (which permanently removed an old security method called getSubject). I had to manually provision OpenJDK 17 and rewrite the system's JAVA_HOME paths to get the JVM engines running smoothly.