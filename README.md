# 🛒 Dmart Sales Analysis using PySpark

## 📌 Project Overview
This project implements a **data analytics pipeline using PySpark** to analyze **Dmart sales data** sourced from multiple datasets.  
The pipeline integrates **product information**, **sales transactions**, and **customer details** to generate meaningful business insights through large-scale data processing.

The project is executed on **Google Colab** using **PySpark**, demonstrating scalable data transformation, integration, and analytical querying.

---

## 🧰 Technologies Used
- **Programming Language:** Python  
- **Big Data Framework:** PySpark  
- **Query Language:** SQL (Spark SQL)  
- **Execution Platform:** Google Colab  
- **Data Format:** CSV  

---

## 📂 Project Structure
dmart-pyspark-analysis/
│
├── content/
│ ├── products.csv
│ ├── sales.csv
│ └── customer.csv
│
├── dmart_analysis.ipynb # PySpark analysis notebook (Google Colab)
└── README.md # Project documentation

---

## 📥 Dataset Description
The project uses three CSV datasets related to Dmart retail operations:

- **products.csv** – Product details (category, sub-category, price, etc.)
- **sales.csv** – Sales transactions (order details, quantity, discount, profit)
- **customer.csv** – Customer information (age, segment, region, state, city)

These datasets are loaded into PySpark DataFrames for processing and analysis.

---

## 🎯 Problem Statement
The objective of this project is to:
- Build a PySpark-based data pipeline
- Integrate multiple datasets using joins
- Perform data cleaning and transformation
- Answer analytical business questions using PySpark and Spark SQL

---

## 🛠️ Task Breakdown

### 🔹 Task 1: Establish PySpark Connection
- Configure PySpark in **Google Colab**
- Initialize `SparkSession`
- Enable reading CSV files into PySpark DataFrames

---

### 🔹 Task 2: Load Data into PySpark DataFrames
- Load the following datasets:
  - `products.csv`
  - `sales.csv`
  - `customer.csv`
- Validate schema and data integrity

---

### 🔹 Task 3: Data Transformation and Cleaning
- Rename columns for consistency
- Handle missing or null values
- Convert columns to appropriate data types
- Join DataFrames using:
  - `Product ID`
  - `Customer ID`
- Create a unified analytical dataset

---

### 🔹 Task 4: Data Analysis and Querying
- Formulate **10 analytical business questions**
- Use PySpark DataFrame API and Spark SQL to answer them
- Aggregate, filter, and group data efficiently

---

## 📊 Analytical Questions Answered

1. What is the total sales for each product category?  
2. Which customer has made the highest number of purchases?  
3. What is the average discount given on sales across all products?  
4. How many unique products were sold in each region?  
5. What is the total profit generated in each state?  
6. Which product sub-category has the highest sales?  
7. What is the average age of customers in each segment?  
8. How many orders were shipped in each shipping mode?  
9. What is the total quantity of products sold in each city?  
10. Which customer segment has the highest profit margin?  

---

## ▶️ How to Run the Project (Google Colab)

1. Open **Google Colab**
2. Upload `dmart_analysis.ipynb`
3. Upload the CSV files into the Colab environment
4. Install and configure PySpark (if required)
5. Run all notebook cells sequentially
6. Review results printed as DataFrame outputs

---

## 📈 Output
- Aggregated tables answering business questions
- Insights on sales, profit, customer behavior, and product performance
- Scalable analysis using distributed processing with PySpark

---

## 🚀 Future Enhancements
- Store processed data in **Hive / HDFS**
- Automate pipeline using **Apache Airflow**
- Visualize insights using **Power BI / Tableau**
- Optimize performance using partitioning and caching
- Deploy on **AWS EMR / Databricks**

---

## 👤 Author
**Gayatri**  
Python Backend Engineer | Aspiring Data Engineer 
