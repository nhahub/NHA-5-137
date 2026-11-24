# 📚 ShelfMind Intelligence Documentation Suite

Welcome to the technical documentation repository for **ShelfMind Intelligence**, an enterprise FMCG Cloud Lakehouse platform engineered on AWS, Apache Airflow, Microsoft Fabric OneLake, and Power BI Direct Lake.

This documentation suite is organized modularly to serve data engineers, solution architects, analytics engineers, and C-level stakeholders.

---

## 📑 Documentation Index

| # | Document | Primary Audience | Key Topics Covered |
| :---: | :--- | :--- | :--- |
| **01** | [**Architecture & Medallion Lakehouse**](./01_ARCHITECTURE_AND_MEDALLION.md) | Architects & Cloud Engineers | S3 Multi-Bucket topology, AWS Glue 5.0 PySpark, Apache Iceberg, OneLake Mirroring, Direct Lake VertiPaq. |
| **02** | [**Pipeline Orchestration & Airflow DAGs**](./02_PIPELINE_ORCHESTRATION.md) | Data Engineers & SREs | CeleryExecutor Airflow setup, 11-stage DAG execution, barrier syncs, preflight health gates. |
| **03** | [**Data Catalog & 56-Defect Taxonomy**](./03_DATA_CATALOG_AND_DEFECTS.md) | Data Quality & Governance | 21-table typed schemas, DQDL checks, formal defect taxonomy, Levenshtein fuzzy matching, ground truth. |
| **04** | [**Star Schema & Fabric Semantic Model**](./04_STAR_SCHEMA_AND_SEMANTIC_MODEL.md) | Analytics Engineers & BI Devs | Kimball Dimensional Modeling (5 Dims, 4 Facts, 4 Marts), 1:* relationships, Direct Lake DAX formulas. |
| **05** | [**Executive Dashboards & UI Playbook**](./05_EXECUTIVE_DASHBOARDS_GUIDE.md) | Product Managers & BI Leads | 4-page Power BI breakdown (Commercial, Inventory, Procurement, DC Operations), KPI math, Dark Cockpit styling. |
| **06** | [**Platinum Prescriptive & ML Engine**](./06_PLATINUM_PRESCRIPTIVE_ENGINE.md) | Data Scientists & ML Engineers | Multi-horizon demand forecasting (7d/14d/30d), weather elasticity, FEFO spoilage prevention, inter-DC transfers. |
| **07** | [**Deployment & Operations Runbook**](./07_DEPLOYMENT_AND_RUNBOOK.md) | DevOps & Platform Engineers | Docker Compose configuration, IAM least privilege policies, Fabric Glue mirroring runbook, troubleshooting. |

---

## 🏛 System Highlights at a Glance

* **Geographic Coverage:** 27 Egyptian Governorates under 3 Distribution Hubs: WH01 (Greater Cairo / 6th of October), WH02 (Alexandria / Coastal), WH03 (Assiut / Upper Egypt).
* **Storage Standard:** Apache Iceberg open-table format providing ACID guarantees, partition evolution, and time-travel querying via AWS Athena v3.
* **Synchronization Latency:** Sub-minute Zero-ETL metadata mirroring into Microsoft Fabric OneLake.
* **Serving Performance:** Direct Lake in-memory VertiPaq engine delivering sub-second response times across 759M+ EGP of transactional sales data.
