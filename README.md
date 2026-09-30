ETL Data Quality Framework

A Python-based data quality validation framework for ETL pipelines, built to mirror checks performed during SIT and UAT in enterprise data engineering environments (Informatica, Ab Initio, Hadoop).

What This Project Does

Validates source-to-target data integrity after an ETL load by running automated checks across both datasets and generating a detailed quality report.

Designed for BFSI / financial data pipelines where accuracy of account balances, currency codes, and referential integrity is critical.

Checks Performed
Check	Description
Row Count Reconciliation	Source and target must have the same number of records
Null Check	Critical columns (account_id, balance, currency) must have zero nulls
Duplicate Detection	Business key (account_id) must be unique in target
Aggregate Reconciliation	SUM of balance must match within ±0.1% tolerance
Value Range / Domain Validation	Currency must be one of: USD, GBP, EUR, SGD, AED
Referential Integrity	All customer_id values in target must exist in the reference table
Project Structure
data-quality-scripts/
│
├── etl_data_quality_validator.py   # Main validation framework
├── requirements.txt                 # Python dependencies
├── sample_data/
│   ├── source.csv                   # Sample source extract
│   ├── target.csv                   # Sample target load (1 intentional defect)
│   └── customers.csv                # Reference lookup table
└── README.md
How to Run

1. Install dependencies

bash
pip install pandas

2. Run the validator

bash
python etl_data_quality_validator.py

Skills Demonstrated
Python (pandas, dataclasses, logging)
ETL data quality validation concepts
Source-to-target reconciliation
SIT / UAT test design for data pipelines
Defect detection and reporting
Git branching and pull request workflow
