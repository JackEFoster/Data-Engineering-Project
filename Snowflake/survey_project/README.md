Welcome to your new dbt project!

### Using the starter project

Try running the following commands:

* dbt run
* dbt test



### Resources:

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
* Find [dbt events](https://events.getdbt.com) near you
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices



DBT PROJECT README

Path: Snowflake/survey\_project/README.md

Survey Project — dbt Models

Overview

This dbt project transforms raw survey data stored in Snowflake into clean, analytics‑ready models. The project follows a layered modeling approach to ensure clarity, maintainability, and scalability.

Modeling Layers

\- Sources

\- Definitions for raw Snowflake tables.

\- Located in models/src.

\- Staging (stg\_)

\- Cleaned, renamed, typed versions of raw tables.

\- One row per entity.

\- Located in models/staging.

\- Intermediate (int\_)

\- Business logic, normalization, category extraction, and transformations.

\- Located in models/intermediate.

\- Marts (fact\_ and dim\_)

\- Final analytics models for dashboards and insights.

\- Located in models/marts.

Project Structure

models/

src/

staging/

intermediate/

marts/

tests/

Running dbt

dbt debug

dbt deps

dbt run

dbt test

Testing Strategy

\- not\_null tests

\- unique tests

\- accepted\_values tests

\- relationships tests

\- custom tests as needed

Conventions

\- snake\_case column names

\- stg\_ prefix for staging models

\- int\_ prefix for intermediate models

\- fact\_ and dim\_ prefixes for marts

\- One model per file

\- No semicolons in SQL models

Environments

dev: DBT\_JACK schema

prod: to be defined

Author

Jack Foster

Data Engineering and Cloud Integration



