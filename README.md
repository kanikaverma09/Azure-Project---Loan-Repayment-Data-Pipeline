

# Loan Repayment Data Pipeline

## Introduction

This repository contains a data pipeline solution named **Loan Repayment Data Pipeline**. The project is designed to ingest and transform loan, loan repayment, and customer details data stored in JSON files within Azure Blob Storage. Using Azure Data Factory and Databricks, the pipeline processes this data to produce meaningful insights and stores the transformed results in Delta format for reporting purposes. The pipeline is scheduled to run daily, ensuring up-to-date data is available for business analytics.

## Business Problem

Financial institutions often need a consolidated view of loan and repayment data to analyze customer behavior, track loan statuses, and maintain accurate financial reporting. Handling multiple data sources, formats, and large datasets can be challenging. Without an efficient pipeline, the risk of data inconsistencies, duplication, and delayed reporting increases, leading to poor decision-making. **Loan Repayment Data Pipeline** solves these problems by providing an automated, reliable pipeline that ensures data quality and timeliness.

## Goal

The main objectives of this project are:
1. **Ingest** loan, repayment, and customer data from JSON files stored in Azure Blob Storage.
2. **Transform** the data to calculate key metrics, such as:
   - Late EMI payment days.
   - Partial payment days.
   - Principal balance left for each loan.
   - Penalty paid for late payments.
3. **Enhance** customer data by splitting location fields and capitalizing customer names.
4. **Clean** data by removing duplicates and handling missing payment reference numbers.
5. **Store** the transformed data in Delta format using upsert logic to avoid loading duplicate records.
6. **Automate** the pipeline to run daily, ensuring the latest data is always processed and available for reporting.
7. **Monitor** and handle any pipeline failures, with automatic retries and email notifications.

## Tools Used

- **Azure Data Factory**: Orchestration of the data pipeline.
- **Azure Blob Storage**: Storage of the source JSON files (Loan, LoanRepayment, Customer).
- **Databricks**: Data transformation and processing.
- **Delta Lake**: Storage of the transformed data using upsert logic to handle duplicates.
- **Azure Monitor**: Monitoring the pipeline execution and sending failure notifications.

## Solution Overview

### Data Ingestion

- **Source Files**: Three JSON files containing loan, repayment, and customer information:
  - `Loan.json`
  - `LoanRepayment.json`
  - `Customer.json`
- The data is ingested from Azure Blob Storage into Databricks for processing.

### Data Transformation

1. **Late EMI Payment Days**: Count the number of late EMI payment days for each loan ID.
2. **Partial Payment Days**: Count the number of partial payment days for each loan ID.
3. **Customer Location**: Split `CustomerLocation` into `CustomerCity`, `CustomerState`, and `CustomerCountry`.
4. **Principal Balance Payment Left**: Calculate the principal balance left by subtracting the total principal paid from the loan amount.
5. **Total Penalty Paid**: Calculate penalties (2.5% of the EMI) for late payments.
6. **Customer Name Formatting**: Capitalize the first letter of each word in `CustomerName`.
7. **Remove Duplicates**: Ensure no duplicate rows exist in the transformed data.
8. **Handle Missing PaymentReferenceNumber**: Replace blank payment reference numbers with `NULL`.

### Data Storage

- The transformed data is stored in **Delta format** in Azure Data Storage.
- **Upsert Logic**: Uses `LoanID` and `BorrowerID` as keys to merge data, ensuring no duplicates are loaded in future pipeline runs. Only specific columns are updated when a match is found:
  - LateEMIPaymntDaysCnt
  - PartitialPymntDaysCnt
  - TotalPenaltyPaid
  - PrincipalBalPymntLeft
  - CustomerCity
  - CustomerState
  - CustomerCountry

### Data Orchestration

- **Daily Schedule**: The pipeline runs every day at 12 AM CST to ensure the latest data is ingested and transformed.
- **Error Handling**: Retry policies are in place for failed activities to handle temporary issues such as network errors or resource unavailability.

### Monitoring and Notifications

- Azure Data Factory's monitoring interface provides real-time status updates on the pipeline execution.
- Email notifications are sent to the data engineering team in case of pipeline failures or data quality issues, allowing for quick troubleshooting.

## Conclusion

The **Loan Repayment Data Pipeline** project is an end-to-end data solution that addresses the challenges of managing loan and repayment data. By automating the data ingestion and transformation process, ensuring data quality, and implementing robust error handling and notifications, the pipeline provides accurate and up-to-date data for business reporting. The use of Delta format and upsert logic ensures data consistency and efficient storage management.

