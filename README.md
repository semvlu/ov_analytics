# OV Analytics: Dutch Public Transport (OV) Analytic Platform

Stream real-time data from NDOV (Nationale Data Openbaar Vervoer) Loket.

PySpark: Store real-time data into Delta Lake, batch process lake data into warehouse.

Prometheus and Grafana for real-time streaming logging and visualisation.

Tools used:

- Container: Docker, Kubernetes
- ETL: Kafka, Spark, ZeroMQ, DuckDB
- Data Lake, Warehouse, OLAP: Delta Lake, DuckDB, Parquet, ClickHouse
- Log & Visualisation: Prometheus, Grafana
- User-end API: TypeScript, Apollo Server, GraphQL

## Spark Job

![alt text](https://github.com/semvlu/ov_analytics/blob/master/preview/spark.png?raw=true)

## Prometheus

![alt text](https://github.com/semvlu/ov_analytics/blob/master/preview/prometheus.png?raw=true)

## Grafana Dashboard

![alt text](https://github.com/semvlu/ov_analytics/blob/master/preview/grafana.png?raw=true)

## GraphQL

![alt text](https://github.com/semvlu/ov_analytics/blob/master/preview/gql.png?raw=true)
