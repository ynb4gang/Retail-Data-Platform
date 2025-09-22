# Retail Data Platform 🏪

A comprehensive data platform for retail analytics built with modern data engineering tools. This platform enables ETL pipelines, data visualization, and business intelligence for retail operations.

## 🚀 Features

* **Data Orchestration**: Apache Airflow for workflow management
* **Data Visualization**: Apache Superset and Metabase for BI dashboards
* **Data Processing**: PostgreSQL for data storage and processing
* **Workflow Automation**: Prefect for data pipeline orchestration
* **Containerized**: Docker-based deployment

## 📊 Architecture

```
Raw Data → ETL Pipelines → Data Warehouse → Visualization
       ↓          ↓               ↓                 ↓
   Sources     Airflow        PostgreSQL        Superset / Metabase
```

## 🛠️ Tech Stack

* **Orchestration**: Apache Airflow 2.6.3
* **BI Tools**: Apache Superset, Metabase
* **Database**: PostgreSQL 15
* **Workflow**: Prefect 2
* **Containerization**: Docker, Docker Compose

## 📋 Prerequisites

* Docker Engine 20.10+
* Docker Compose 2.0+
* 4GB RAM minimum
* 10GB free disk space

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone git clone https://github.com/ynb4gang/Retail-Data-Platform.git
cd retail-platform
```

### 2. Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit with your preferences (optional)
nano .env
```

### 3. Start the Platform

```bash
# Start all services
docker-compose up -d

# Check services status
docker-compose ps
```

### 4. Access the Services

| Service         | URL                                            | Default Credentials         |
| --------------- | ---------------------------------------------- | --------------------------- |
| Apache Airflow  | [http://localhost:8080](http://localhost:8080) | admin / admin               |
| Apache Superset | [http://localhost:8088](http://localhost:8088) | admin / admin               |
| Metabase        | [http://localhost:3000](http://localhost:3000) | Set on first access         |
| Prefect         | [http://localhost:4200](http://localhost:4200) | -                           |
| PostgreSQL      | localhost:5432                                 | retail\_user / retail\_pass |

## 📁 Project Structure

```
retail-platform/
├── dags/                 # Airflow DAGs
├── plugins/              # Airflow custom plugins
├── logs/                 # Airflow logs
├── superset_home/        # Superset configuration
├── init/                 # Database initialization scripts
├── docker-compose.yml    # Service orchestration
└── .env                  # Environment variables
```

## 🔧 Configuration

### Database Connections

* **Host**: postgres (within Docker) or localhost (external)
* **Port**: 5432
* **Database**: retail\_db
* **Username**: retail\_user
* **Password**: retail\_pass

### Airflow Connections

Create in Airflow UI (Admin → Connections):

* **Connection ID**: postgres\_retail
* **Type**: Postgres
* **Host**: postgres
* **Schema**: retail\_db
* **Login**: retail\_user
* **Password**: retail\_pass
* **Port**: 5432

## 📊 Sample ETL Pipelines

* **Raw to Staging (`raw_to_staging`)**

  * Loads data from raw tables to staging area
  * Scheduled daily
  * Includes data validation

* **Staging to DWH (`staging_to_dwh`)**

  * Transforms staging data to data warehouse
  * Dimension and fact table loading
  * Triggered by staging completion

* **Manual Full Load (`manual_full_load`)**

  * One-time data loading
  * Complete ETL process
  * Manual trigger

## 🗃️ Data Model

### Core Schemas

* **raw**: Raw data ingestion
* **staging**: Data cleaning and validation
* **dwh**: Data warehouse (dimensions + facts)

### Key Tables

* **dim\_product** - Product master data
* **dim\_store** - Store information
* **fct\_sales** - Sales transactions
* **fct\_competitors** - Competitor pricing
* **fct\_promotions** - Marketing campaigns

## 🔄 Operations

### Starting Services

```bash
docker-compose start
```

### Stopping Services

```bash
docker-compose stop
```

### Viewing Logs

```bash
# Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Specific service logs
docker-compose logs [service-name]

# Follow logs in real-time
docker-compose logs -f [service-name]
```

### Restarting Services

```bash
# Restart specific service
docker-compose restart [service-name]

# Restart all services
docker-compose restart
```

### Database Management

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U retail_user -d retail_db

# Backup database
docker-compose exec postgres pg_dump -U retail_user retail_db > backup.sql

# Restore database
docker-compose exec -T postgres psql -U retail_user -d retail_db < backup.sql
```

## 🐛 Troubleshooting

### Common Issues

* **Port conflicts:**

```bash
lsof -i :8080
```

Change ports in `docker-compose.yml` if needed.

* **Service health checks:**

```bash
docker-compose ps
docker-compose logs [service-name] --tail=50
```

* **Database connection issues:**

```bash
docker-compose exec postgres pg_isready -U retail_user
docker-compose exec postgres psql -U retail_user -d retail_db -c "\dt"
```

* **Reset Platform:**

```bash
docker-compose down -v
docker-compose up -d
```

## 📈 Monitoring

* **Airflow**: DAG runs, task status, SLA monitoring
* **PostgreSQL**: Query performance, connection pool
* **Superset**: Dashboard performance, query logs
* **System**: Resource usage via Docker stats

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙋‍♂️ Support

For support and questions:

* Create an issue in the repository
* Check existing documentation
* Review service-specific logs

## 🔮 Roadmap

* Add real-time data streaming
* Implement data quality checks
* Add machine learning pipelines
* Enhanced monitoring and alerting
* Multi-environment deployment

> **Note**: This platform is designed for development and testing. For production deployment, additional security and performance configurations are required.

<div align="center">
Built with ❤️ for retail data analytics
</div>

---

# 📚 Additional Documentation

## 🎯 Quick Commands Cheat Sheet

### Service Management

```bash
# Start all services
docker-compose up -d

# Stop all services  
docker-compose stop

# View service status
docker-compose ps

# View logs
docker-compose logs -f

# Restart specific service
docker-compose restart airflow-webserver
```

### Database Operations

```bash
# Connect to DB
docker-compose exec postgres psql -U retail_user -d retail_db

# List tables
\dt

# Query data
SELECT * FROM retail.sales LIMIT 5;
```

### Airflow Operations

```bash
# List DAGs
docker-compose exec airflow-webserver airflow dags list

# Trigger DAG
docker-compose exec airflow-webserver airflow dags trigger manual_full_load

# Pause/Unpause DAG
docker-compose exec airflow-webserver airflow dags pause raw_to_staging
```

## 🔐 Default Credentials Summary

| Service    | URL                                            | Username     | Password     | Notes          |
| ---------- | ---------------------------------------------- | ------------ | ------------ | -------------- |
| Airflow    | [http://localhost:8080](http://localhost:8080) | admin        | admin        | Full access    |
| Superset   | [http://localhost:8088](http://localhost:8088) | admin        | admin        | BI dashboards  |
| Metabase   | [http://localhost:3000](http://localhost:3000) | set on first | -            | Alternative BI |
| PostgreSQL | localhost:5432                                 | retail\_user | retail\_pass | Database       |
| Prefect    | [http://localhost:4200](http://localhost:4200) | -            | -            | Workflow UI    |

## 🗂️ File Structure Details

### DAGs Directory (`dags/`)

* `manual_full_load.py` - One-time ETL pipeline
* `raw_to_staging.py` - Daily raw data processing
* `staging_to_dwh.py` - Data warehouse loading

### Configuration Files

* `.env` - Environment variables
* `docker-compose.yml` - Service definitions
* `superset_config.py` - Superset settings

### Data Directories

* `logs/` - Airflow execution logs
* `plugins/` - Custom Airflow plugins
* `superset_home/` - Superset metadata

## 🚨 Important Notes

### Data Persistence

* PostgreSQL data persists in Docker volume
* Airflow metadata stored in PostgreSQL
* Superset configurations in `superset_home/`

### Security Considerations

* Change default passwords in production
* Use SSL for database connections
* Restrict network access appropriately
* Regular backups recommended

### Performance Tips

* Allocate sufficient RAM to Docker
* Monitor disk space for logs
* Adjust PostgreSQL memory settings
* Use connection pooling for heavy loads

## 🔄 Typical Workflow

```
Data Ingestion → Raw tables in PostgreSQL
ETL Processing → Airflow DAGs transform data
Data Warehouse → Cleaned data in DWH schema
Visualization → Superset/Metabase dashboards
Monitoring → Track pipeline health and performance
```

## 📞 Getting Help

Check service-specific documentation:

* [Airflow Documentation](https://airflow.apache.org/docs/)
* [Superset Documentation](https://superset.apache.org/docs/)
* [Metabase Documentation](https://www.metabase.com/docs/)
* [PostgreSQL Documentation](https://www.postgresql.org/docs/)

For platform-specific issues, check the logs and ensure all services are healthy before troubleshooting individual components.
