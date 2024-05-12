# Real-Time-Stress-Detection
Real-time stress detection framework harnessing Apache Spark and Apache Kafka.

The aim of this project is to develop a real-time stress detection system using physiological signals, specifically Heart Rate Variability (HRV) and Electrodermal Activity (EDA). 
The system is designed to continuously monitor these signals and detect instances of stress in real-time. This serves a wide range of applications, including stress management, mental health monitoring, and intervention systems.

Apache Kafka is employed to act here as a buffer, decoupling data ingestion (from APIs of wearable devices) from data processing, and ensuring data durability and reliability. Apache Spark DataFrames enable efficient processing of physiological data, while Spark Structured Streaming ensures fault-tolerant and scalable stream processing. The predictive models are optimized by Recall, with scores > 0.95.
