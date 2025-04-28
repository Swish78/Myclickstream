# Clickstream Pipeline with ML User Segmentation

This project implements an end-to-end data pipeline for processing web clickstream data with machine learning integration. It collects user interaction events, processes them using distributed computing frameworks, and applies K-Means clustering to segment users based on their behavior patterns.

## Project Overview

The pipeline consists of five phases:

1. **Event Simulation & Ingestion**: Generates synthetic clickstream events and publishes them to Kafka
2. **Batch Processing**: Transforms raw data into user features using PySpark
3. **ML Model - User Segmentation**: Applies K-Means clustering to segment users
4. **Scheduling & Orchestration**: Automates pipeline execution with Apache Airflow
5. **Analytics & Dashboards**: Enables data exploration with AWS Athena and visualization tools

## Architecture

```
Raw Events -> Kafka -> S3 (Raw) -> PySpark -> S3 (Features) -> PySpark MLlib -> S3 (User Segments) -> Athena/Dashboards
```

## Directory Structure

```
clickstream_pipeline/
├── data_generator/
│   └── event_producer.py  # Generates synthetic clickstream events
├── spark_jobs/
│   ├── transform_clickstream.py  # Transforms raw data to features
│   └── user_segmentation_ml.py   # Runs K-Means clustering
├── airflow_dags/
│   └── clickstream_pipeline_dag.py  # DAG definition
├── config/
│   └── s3_paths.yaml  # Configuration for data paths
└── dashboard/
    └── streamlit_app.py  # Visualization dashboard
```

## Key Components

### Event Generator

The event generator creates realistic clickstream data that simulates user behavior on a website, including:
- Page views across different sections
- User interactions (clicks, searches, add-to-cart)
- Session tracking with timestamps
- User properties (device, location, etc.)

### PySpark Data Processing

The data processing pipeline:
- Parses and validates raw JSON events
- Extracts temporal features (time of day patterns, session duration)
- Creates user behavioral profiles (pages viewed, interaction frequency)
- Aggregates data for machine learning

### User Segmentation with K-Means

The ML component:
- Uses PySpark MLlib to scale clustering to large datasets
- Identifies distinct user segments based on behavior
- Creates user-to-segment mappings for targeted analytics
- Provides interpretable cluster centers for business insights

## Getting Started

### Prerequisites

- Python 3.8+
- Apache Kafka
- Apache Spark 3.0+
- Apache Airflow 2.0+
- AWS account (for S3, Athena)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/clickstream-pipeline.git
   cd clickstream-pipeline
   ```

2. Set up Python environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Configure AWS credentials:
   ```
   aws configure
   ```

### Running the Pipeline

#### 1. Start the Event Generator

```bash
cd data_generator
python event_producer.py --bootstrap-servers localhost:9092 --topic clickstream --duration 120
```

#### 2. Run the PySpark Transformation Job

```bash
cd ../spark_jobs
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
  transform_clickstream.py \
  --input-path s3://clickstream/raw/ \
  --output-path s3://clickstream/features/ \
  --start-date 2025-04-01 \
  --end-date 2025-04-21
```

#### 3. Run the User Segmentation Job

```bash
spark-submit user_segmentation_ml.py \
  --features-path s3://clickstream/features/ \
  --output-path s3://clickstream/user_segments/ \
  --num-clusters 5
```

#### 4. Deploy the Airflow DAG

Copy the DAG file to your Airflow DAGs directory:

```bash
cp ../airflow_dags/clickstream_pipeline_dag.py ~/airflow/dags/
```

## Configuration

Edit `config/s3_paths.yaml` to customize data storage locations:

```yaml
raw_data: s3://clickstream/raw/
features: s3://clickstream/features/
user_segments: s3://clickstream/user_segments/
```

## Visualization & Analytics

Run the Streamlit dashboard:

```bash
cd ../dashboard
streamlit run streamlit_app.py
```

## Future Enhancements

- Real-time processing with Spark Streaming
- A/B testing integration
- Recommendation system based on user segments
- Anomaly detection for monitoring data quality

## License

This project is licensed under the MIT License - see the LICENSE file for details.