Bank Fraud Detection System Documentation

Project Overview

A simulation of a banking system that receives transactions as streaming events, analyzes them in real time, and displays suspicious activity alerts along with an understandable explanation for each alert to support reviewer assessment.

Data Processing Pipeline

1. The system generates synthetic transactions at a rate of 5 transactions per second.
2. Each transaction is sent in JSON format to a Kafka topic named "bank-transactions".
3. Spark Structured Streaming consumes the events in 5-second micro-batches.
4. Spark applies large-amount and transaction-frequency detection rules, then calculates the fraud probability using an XGBoost model.
5. Raw events are stored as Parquet files in Hadoop HDFS, partitioned by country.
6. Dashboard data and alert results are stored in a lightweight SQLite database shared with the FastAPI dashboard.

Predictive Model

- Algorithm: XGBoost Classifier.
- Number of Trees: 400.
- Maximum Depth: 4.
- Learning Rate: 0.1637.
- Input Features: 29 features ("V1" through "V28", plus "Amount").
- Fraud Classification Threshold: 65%.
- Training Metric: AUC-PR, which is suitable for imbalanced datasets such as fraud detection.
- Experimental Validation Accuracy: 100% on 83 demo cases within the project (80 legitimate and 3 fraudulent).

«The 100% demo accuracy demonstrates the correctness of the software integration and processing pipeline only; it does not represent a production-level model evaluation. A proper evaluation requires an independent test dataset and performance metrics such as Precision, Recall, F1-score, and PR-AUC.»

Explainable Detection Rules

- High-Value Transaction: A transaction amount equal to or greater than 10,000.
- Rapid Transaction Frequency: Four or more transactions made by the same customer within 10 minutes.
- Machine Learning Model: A fraud probability equal to or greater than 65%.

Project Demonstration Scenario

1. Open the monitoring dashboard and select "Mixed Scenarios".
2. Click "Start System" and observe that the Kafka message counter advances ahead of the Spark processing counter.
3. Explain that this difference represents a normal buffer between the message producer and consumer.
4. Open the Kafka UI to demonstrate the topic and its three partitions.
5. Open the Hadoop NameNode interface to show the Parquet files and checkpoint data.
6. Return to the dashboard and explain the reason behind each alert and the corresponding model score.

Academic Version Limitations

- The data is synthetically generated and does not consist of real banking transactions.
- Authentication and message encryption are not implemented in the local demonstration environment.
- Dashboard results are stored using lightweight storage, while raw analytical data is stored in HDFS.
- Before production deployment, the model must be retrained, data drift must be monitored, access controls must be implemented, and end-to-end encryption must be enforced.