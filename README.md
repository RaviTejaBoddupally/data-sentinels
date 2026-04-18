# data-sentinels

### Data Engineering project
- Data Set : Brazilian E Commerce 
- Description : This is an e-comm dataset with 9 files which helps us to explore joins and aggregations.
=======
# Olist Customer Segmentation Pipeline

## Overview
This project implements an end-to-end Data Engineering pipeline using **PySpark** combined with the power of **Databricks** leveraring **delta live tables**. 
## Dataset Description
E-commerce is ever growing field often leveraging the power of data analytics to major effect. With this as the primary motivation we have identified OList E-commerce data as our dataset. This dataset contains 9 files from orders, order items, products to review of the products. The brief intro for each of data is provided below.

## Questions answered
As a team we have identified below few oppurtunities to improve the business for Olist
- Customer segmentaion
- Logistics and delivery management
- Revenue statistics

## Oppurtunity 1 - Customer Segmentation
In this oppurtunity we wanted to focus on utilizing last 12 months of active purchasing behavior, the pipeline aggregates total spend and order frequency, ranks customers by percentiles, and categorizes them into actionable business personas (e.g., VIP, Core Customer, Casual Buyer) using a 3x3 segmentation matrix.

## Architecture
The pipeline strictly follows the **Medallion Architecture** (Bronze, Silver, Gold), leveraging DLT's declarative Directed Acyclic Graph (DAG) for automated dependency management. Code is logically modularized into separate files based on their respective layers to maintain a clean workspace.

---

## 🥉 Bronze Layer (Raw Ingestion)
This layer performs raw data ingestion from the source files into Materialized Views. 
* **`raw_geolocation`**: Contains geographical coordinates and zip codes across Brazil.
* **`raw_customers`**: Stores unique customer IDs and their geographic location metadata.
* **`raw_olist_sellers`**: Contains seller IDs, zip codes, and location details.
* **`raw_orders`**: Holds core order metadata, including order status and purchase timestamps.
* **`raw_order_items`**: Contains line-item details for each order, including individual item price and freight value.
* **`raw_order_payments`**: Records payment methods, installments, and total transaction values per order.
* **`raw_order_reviews`**: Stores customer reviews, scores, and review timestamps for completed orders.
* **`raw_product_name_translations`**: Maps Portuguese product category names to English.
* **`raw_products`**: Product catalog detailing category, name length, and physical dimensions.

---

## 🥈 Silver Layer (Staging & Transformations)
The Silver layer cleans, filters, and aggregates the raw data into optimized intermediate tables for our segmentation logic.
* **`stg_orders_agg`**: Aggregates item-level financial data (price + freight) up to the summary order level.
* **`stg_last_12months_orders`**: Joins customers and orders, filtering strictly for "delivered" orders within a dynamic 12-month active window (calculated via an optimized single-row cross-join anchor date).
* **`stg_cust_percentile`**: Uses PySpark window functions to calculate the monetary spend percentile rank for each unique customer.
* **`stg_cust_vol_agg`**: Aggregates the total order volume (purchasing frequency) per unique customer.

---

## 🥇 Gold Layer (Business Value)
The Gold layer contains the final, business-ready tables optimized for BI dashboards, data visualization, and downstream marketing systems.
* **`customer_segmentation`**: The final materialized view. It joins the customer percentile and volume aggregates, applying PySpark conditional logic (`when`/`otherwise`) to assign each customer into final Value (Low/Medium/High) and Volume (Low/Medium/High) tiers, ultimately yielding our target business personas.

## Key Technologies
* **Databricks / Delta Lake**: Scalable data storage and distributed processing.
* **Delta Live Tables (DLT)**: Declarative pipeline orchestration and DAG generation.
* **PySpark**: DataFrame transformations, window functions, and conditional logic.
>>>>>>> Stashed changes
