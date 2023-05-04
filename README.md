# Github Data Pipeline

Reproducible end-to-end data pipeline to transform ecommerce purchase data into actionable insights

### Table of contents
- [Objective](#introduction)
- [Use Cases](#use-cases)
- [Dashboard](#dashboard)
- [Tools & Data](#tools-data)
- [Implementation Plan](#implementation-plan)
---

## Objective

As a data engineer, my objective is to design and implement a robust data pipeline that extracts data from the GitHub Archive, transforms it to fit a BigQuery data model, and loads it into BigQuery tables for easy querying and analysis.

In addition to creating the pipeline, I will also be responsible for designing and building a web-based dashboard that provides real-time insights into the data. This dashboard will enable organizations to monitor their GitHub activity and identify trends and patterns that can help improve their development processes.

The key steps involved in this project include:
- Data Extraction: Extracting the relevant data from the GitHub Archive using the GitHub API and other tools.
- Data Transformation: Transforming the data to fit a BigQuery schema, which involves cleaning, aggregating, and enriching the data as necessary.
- Data Loading: Loading the transformed data into BigQuery tables for efficient querying and analysis.
- Dashboard Design and Implementation: Designing and building a monitoring dashboard that provides real-time insights into the data, using tools such as Google Data Studio or Tableau.

---

## Use Cases
- User Story 1: As a development team lead, I want to be able to monitor our team's GitHub activity over time and review with my team, so that I can identify issues and bottlenecks and make data-driven decisions to improve our development processes.
- User Story 2: As a product manager, I want to be able to analyze our organization's GitHub activity over time, so that I can understand trends and identify areas for improvement.
- User Story 3: As a data analyst, I want to be able to query and analyze our organization's GitHub data, so that I can answer specific questions and generate custom reports about our main activities (commits/pull requests)
---

## Dashboard
![github-data-pipeline](images/Dashboard.png)
---

## Tools & Data 
### **Dataset**

### **Tools** 
Diagram

### **Implementation Plan** 

- Set up a Google Cloud Platform (GCP) account and create a project.
- Provision Virtual Machine (VM) instances to run the necessary infrastructure.
- Use Terraform to create a network infrastructure
- Create a Google Cloud Storage bucket to store the Kaggle dataset files.
- Use Pandas to extract the data from the GH Archive datasets and load it into BigQuery using the cli commands from gcloud.
- Set up Prefect for data orchestration to schedule and manage the pipeline workflow. Use Docker to containerize the pipeline components for portability and consistency for this step.
- Use Dbt to transform and model the data in BigQuery into a structured format for reporting.
- Set up Looker studio to connect to BigQuery and visualize the data.


