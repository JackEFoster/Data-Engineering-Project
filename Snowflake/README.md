ROOT‑LEVEL README
Path: Snowflake/README.md
Survey Data Engineering Project
Overview
This repository contains the full end‑to‑end data engineering workflow for processing gym survey data using Snowflake and dbt. The project includes raw data loading, transformation pipelines, environment setup, and analytics‑ready modeling.
Architecture
- Raw Layer
- Raw survey data is loaded into Snowflake under the customer_surveys.raw_data schema.
- Loading scripts are stored in this repository.
- Transformation Layer (dbt)
- The dbt project lives in the survey_project directory.
- dbt handles staging, cleaning, intermediate modeling, and final analytics models.
- Analytics Layer
- Cleaned models are materialized into the DBT_JACK schema for exploration and dashboarding.
Repository Structure
Loading_Survey_Data_v3.sql        Raw data loading script
survey_project/                   dbt project
dbt_env/                          Local Python environment (ignored)
logs/                             dbt logs (ignored)
.gitignore                        Git ignore rules
README.md                         This file
How to Run the Project
- Create and activate the Python environment
python -m venv dbt_env
dbt_env\Scripts\activate
- Install dbt
pip install dbt-snowflake
- Configure Snowflake credentials in ~/.dbt/profiles.yml
- Run dbt
dbt debug
dbt run
dbt test
Branching Strategy
main       Production-ready code
develop    Integration branch
jack/feature/...   Individual feature branches
Commit Style
feat: new model or feature
fix: bug fix
refactor: cleanup or restructuring
docs: documentation updates
chore: environment or repo maintenance
Author
Jack Foster
Data Engineering and Cloud Integration