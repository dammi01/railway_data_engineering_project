# Railway Data Engineering Project

This end-to-end data engineering project demonstrates the full lifecycle of data, from raw ingestion to analytical consumption. Focusing on real-world railway traffic data from the Netherlands, the project is structured as a series of modular "Lego pieces," allowing for step-by-step learning, clear documentation, and easy extensibility.

**Key Features:**

* **Modular Architecture:** Designed with a Medallion Architecture (Bronze, Silver, Gold layers) for clear separation of concerns and maintainability.
* **Data Lakehouse Approach:** Leverages **DuckDB with Delta Lake (via `ducklake` extension)** for efficient, transactional, and scalable local data storage and querying.
* **Modern Tooling:** Utilizes **Python, Docker, and dbt** for robust data pipeline development, transformation, testing, and documentation.
* **Automation:** Orchestrated using **Apache Airflow** for automated data flows.
* **Scalability Mindset:** Built with future cloud deployment (e.g., MotherDuck, BigQuery) in mind, ensuring concepts like partitioning and optimized data formats are foundational.

**Goal:** To build a comprehensive data engineering portfolio piece, solidifying best practices in data pipeline design, data quality, and scalable analytics, all while fostering in-depth learning.
