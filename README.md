# 🏎️ Formula 1 Data Engineering Project on Azure Databricks

📅 **Period:** March 2025 - April 2025  
🌍 **Location:** São Paulo, Brazil  
🔧 **Technologies:** Databricks, Azure, Apache Spark, PySpark, SQL, Delta Lake, Data Factory, Power BI, Unity Catalog

---

## 🚀 Project Overview

This project applies **cloud data engineering best practices** using real-world **Formula 1 data from 1950 onwards**, leveraging tools from the Azure ecosystem and Apache Spark.

Throughout the project, we developed the core skills required for the **Azure Data Engineer Associate (DP-203)** certification, focusing on governance, ingestion, transformation, analysis, and visualization.

---

## 🧠 Skills Acquired

- ✅ Data ingestion and transformation with **PySpark**
- ✅ Querying and analysis using **Spark SQL**
- ✅ Dimensional modeling with **Star Schema**
- ✅ Efficient storage with **Data Lake Gen2** and **Delta Lake**
- ✅ Orchestration with **Azure Data Factory (ADF)**
- ✅ Data governance using **Unity Catalog, Azure Key Vault, and Azure AD**
- ✅ Dashboards and reporting with **Databricks and Power BI**
- ✅ Modern architectures: **Data Lake** & **Lakehouse**

---

## 🧰 Tools & Technologies Used

| Technology             | Main Role                                           |
|------------------------|-----------------------------------------------------|
| **Azure Databricks**   | Notebooks, job scheduling, dashboards               |
| **Apache Spark / PySpark** | Distributed data processing and transformation |
| **Delta Lake**         | Optimized storage with incremental updates          |
| **Azure Data Lake Gen2** | Data lake for raw and processed data               |
| **Azure Data Factory** | Orchestration and automation of data pipelines      |
| **Power BI**           | Reporting and visualization                         |
| **Unity Catalog**      | Governance and centralized data catalog             |
| **Azure Key Vault**    | Secure credential and secret management             |
| **Azure Active Directory** | User authentication and access control         |

---

## 🧩 Solution Architecture

```
                     +-------------------------+
                     |   Azure Data Factory    |
                     |   (Orchestration Layer) |
                     +-----------+-------------+
                                 |
          +----------------------+-------------------------+
          |                                                |
          ▼                                                ▼
 +-----------------------+                 +------------------------------+
 | Trigger Databricks    |                 | Monitor & Manage Pipelines  |
 | Jobs via ADF Pipelines|                 | and Schedule Increments     |
 +----------+------------+                 +-------------+----------------+
            |                                               
            ▼                                                                    
+----------------------------+                         
|      Azure Databricks      |  ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ←  
| (Notebooks: PySpark & SQL) |        Development, ELT & Data Analysis  
+----------------------------+                         
            |                                         
            ▼                                         
    +---------------+
    |  Bronze Data  |
    |   Container   |
    |  (Delta Lake) |
    +---------------+
            |
            |
            ▼
    +---------------+
    |  Silver Data  |
    |   Container   |
    |  (Delta Lake) |
    +---------------+
            |
            |
            ▼
    +---------------+
    |   Gold Data   |
    |   Container   |
    |  (Delta Lake) |
    +---------------+
            |
            |
            ▼                                      
    +---------------------+                           
    |    Power BI / BI    |  ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ← ←  
    | Dashboards & Reports|     Data Visualizations & Insights   
    +---------------------+
```

---

## 🛠️ Project Execution Steps

### 1. 🔧 Environment Setup
- Created **Databricks clusters**, **notebooks**, **dashboards**, and **jobs**
- Integrated with **Azure Data Lake Gen2**
- Configured **Unity Catalog**, **Azure AD**, and **Azure Key Vault** for secure access and governance

### 2. 📥 Data Ingestion
- Collected historical Formula 1 data (CSV/JSON format)
- Ingested raw data into the **Bronze layer** using PySpark in Databricks

### 3. 🔄 Data Transformation & Modeling
- Transformed and cleaned data using PySpark
- Stored processed data in **Delta tables** in **Silver** and **Gold** layers
- Implemented **partitioning** and **incremental load** techniques for performance

### 4. 🧪 Data Analysis with Spark SQL
- Created **external tables** and **temporary views**
- Performed analysis using Spark SQL on races, drivers, teams, and standings

### 5. 📊 Data Visualization
- Published **interactive dashboards in Databricks**
- Connected **Power BI** to Databricks for advanced reporting and sharing

### 6. 📆 Pipeline Orchestration with ADF
- Built **ADF pipelines** to trigger Databricks notebooks
- Implemented **triggers and monitoring** for weekly updates with alerts

### 7. 🔐 Data Governance
- Configured **Unity Catalog** to manage metadata and access policies
- Applied **role-based access controls** using Azure AD
- Used **Key Vault** for secure secret management

---

## 📉 Results Achieved

- 🔄 **87% reduction** in pipeline processing time with incremental loading  
- 📊 Full analytical coverage of Formula 1 history from 1950 onward  
- ✅ Modern **Lakehouse architecture** implementation  
- 🔐 Strong data governance using best practices and Azure-native tools

---

## 🧑‍💻 Ideal For

✔️ Data engineering professionals working with **Azure Databricks**  
✔️ Individuals preparing for the **DP-203 Azure Certification**  
✔️ Anyone looking to build a **real-world, end-to-end data pipeline**

---

## 📎 Prerequisites

- Azure subscription with access to **Databricks**, **Data Lake Gen2**, and **Data Factory**  
- **Power BI Desktop** (optional, for local visualization)  
- Basic knowledge of **Python, SQL, and Spark**

---

## 🏁 Final Thoughts

This project demonstrates how to **design, manage, and orchestrate cloud data pipelines** from raw ingestion to final dashboards—using real, rich historical Formula 1 data. It's a powerful example of combining engineering, analytics, and governance in a real-world scenario.
