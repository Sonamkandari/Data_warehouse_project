## 🟫 Bronze Layer — Summary of Work Completed

The Bronze layer is the **raw data ingestion layer** of the data warehouse. It stores data exactly as received from the CRM and ERP source systems without any transformation.

---

### ✅ 1️⃣ Analyze Source Systems (CRM & ERP)
- Reviewed all CSV files provided by the CRM and ERP systems.  
- Understood all columns, data types, and table structures.  
- Mapped how each file will be represented in the Bronze layer.

---

### ✅ 2️⃣ Design & Build the Bronze Layer Structure
- Created the **DataWarehouse** database.
- Defined schemas: **bronze**, **silver**, **gold**.
- Built DDL tables for the Bronze layer matching the exact structure of the source CSVs.
- Ensured the Bronze layer remains a **raw, unmodified copy** of the source systems.

---

### ✅ 3️⃣ Load Raw Data into Bronze (ETL: Extract & Load)
- Wrote SQL `BULK INSERT` scripts to ingest CRM and ERP CSV files.
- Developed the stored procedure **`bronze.load_bronze`** to automate:
  - Truncating tables before loading  
  - Loading all CSV files  
  - Logging execution steps  
  - Handling errors using TRY/CATCH  

---

### ✅ 4️⃣ Add Monitoring & Performance Tracking
- Added variables to capture:
  - **@start_time** — when each table load begins  
  - **@end_time** — when the load completes  
- Computed:
  - **Load duration per table (seconds)**  
  - **Total batch duration for the Bronze layer**
- Implemented print logs to trace ETL progress and improve debugging.

---
