ETL  Data Pipeline with Apache Airflow

📌 Overview
- This implements an **ETL (Extract, Transform, Load) workflow** using **Apache Airflow** to automate employee data processing.
- The pipeline fetches raw employee data from postgres database, processes it to compute tax and bonus percentage, and saves the results for reporting or downstream use.
- The Airflow DAG uses the SMTP settings defined in airflow.cfg file.
- Each task inherits default_args, which specifies the email recipients and when alerts should be sent.  
- If a task/task instance fails, Airflow automatically sends an alert email. 
- Immediate alerts minimize delays in data availability, keeping downstream systems and reports accurate. 
- You don’t have to keep checking the Airflow web UI — alerts will reach you automatically!

⚙️ Workflow
The ETL pipeline consists of three main tasks orchestrated by an Airflow DAG:

1. *Fetch Data* – Extracts employee records from the postgres database.  
2. *Process Data* – Transforms the data to compute tax and bonus percentage.  
3. *Save Results* – Load the processed and computed data into storage.  

🗓️ Scheduling
- The DAG is scheduled to run multiple times a day (configurable in Airflow).  
- Example: Runs at 2:30 PM, 2:45 PM, and 3:00 PM IST (09:00, 09:15, 09:30 UTC).

Author:
**Dinesh Deekkan**
This project was developed during my internship at **C-DAC INDIA (Centre for Development of Advanced Computing)** as part of learning and practicing ETL automation with Apache Airflow.
