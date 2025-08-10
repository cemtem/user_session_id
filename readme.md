# Data Processing Project

## Overview

This project implements a data processing pipeline using Apache Airflow for orchestration.

## Project Structure

- `infra/` - Contains Docker and infrastructure configuration files
- `dags/` - Airflow DAG definitions
- `scripts/` - Utility scripts including data generator
- `src/` - Source code for data processing
- `data_generator/` - Data generator
- `data_warehouse/` - Where data will be stored, mocking real DW
- `source_data/` - Location for generated source data
- `jupyter/` - notebooks to check results

## Prerequisites

- Docker and Docker Compose
- Python 3.11.9
- Virtual environment with required packages

## Installation and Setup

1. Clone the repository
2. infra/ docker compose up -d
3. data_generator/generator.py - run to generate source data
4. localhost:8090 - airflow UI(airflow/airflow), run DAG with params(start date and end date have to be within period generated in file with as_of date)
5. you can easily check the results by running jupyter notebook [gold.ipynb](jupyter/gold.ipynb)
6. calculation is done in [gold.py](spark_app/events_table/gold.py). It takes rows within specified period, looks for affected dates, takes affected date from current gold table and aggregates through it
