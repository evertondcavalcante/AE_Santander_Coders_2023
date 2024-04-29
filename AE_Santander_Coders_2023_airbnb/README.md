### Data Engineering Project: Airbnb Data Quality
Welcome to your new dbt project!


### Introduction
This project is a collaboration between five members to enhance the data quality from the Airbnb API. Our goal is to perform transformations, cleanings, and ensure data integrity across various layers. This README provides an overview of the project and the steps followed by the team.

### Airbnb Dataset Overview
The "Inside Airbnb" dataset provides insights into accommodation listings, guest reviews, and calendar availability in various cities, including Rio de Janeiro. Key components include property details, pricing, and location.

### Project Steps
1. **Data Acquisition and PostgreSQL Storage - Bronze Layer:** We downloaded the Inside Airbnb data and structured it in a PostgreSQL database, storing the "Listing," "Reviews," and "Calendar" tables in the public schema.
2. **Data Quality - Silver Layer:** We defined quality metrics for the "bronze" layer data and implemented automated checks to ensure compliance.
3. **Quality Testing - Silver Layer:** Leveraging the Great Expectations library, we created automated tests to verify expectations set for the data.
4. **Data Transformation with dbt - Gold Layer:** Utilizing dbt, we crafted the "gold" data layer, applying specialized transformations and aggregations for deeper analysis.


### Data Storage:
All layers are stored in PostgreSQL tables within the public schema, as follows
* Bronze: 1_calendar_bronze.sql, 1_listings_bronze.sql, 1_reviews_bronze.sql
* Silver: 2_calendar_silver.sql, 2_listings_silver.sql, 2_reviews_silver.sql
* Gold: 3_detailed_amenities_impact_gold.sql, 3_distribuicao_tipos_propriedades_gold.sql, 3_high_value_copacabana_listings_gold.sql, 3_host_response_time_gold.sql, 3_low_score_rating_listings_gold.sql, 3_neighborhood_avarage_prices_gold.sql, 3_overview_listings_price_neighbourhood_rating_gold.sql, 3_tendencias_satisfacao_gold.sql, 3_visao_sazonal_gold.sql
* Non-Null Data (Missing): 4_calendar_missing.sql, 4_listings_missing.sql, 4_reviews_missing.sql

### Getting Started with the Starter Project
Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
