# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, year, quarter, to_date, month, count_distinct
from plotly.graph_objects import Figure, Scatter, Bar, Layout
import networkx as nx
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBRegressor
import plotly.express as px
import pickle
import logging
import webbrowser
import os
import time
from scipy import stats  # For T-Test
from fpdf import FPDF  # For PDF report generation
import random  # For random color assignment

# Print initial message to inform user about execution time
print("Program is running... This may take approximately 1-2 minutes depending on your system and data size.")

# Suppress PySpark and other module logs to ERROR level
spark_log = SparkSession.builder.getOrCreate()._jvm.org.apache.log4j
spark_log.LogManager.getRootLogger().setLevel(spark_log.Level.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)

# Initialize SparkSession with optimized settings for handling larger datasets
spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.default.parallelism", "50") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .getOrCreate()

# Load Store Sales dataset into a DataFrame
try:
    df = spark.read.option("header", "true").csv("C:/Projects/data/superstore_data_extended.csv")
except Exception as e:
    print(f"Error loading dataset: {e}")
    spark.stop()
    exit(1)

# Remove rows with missing values
df_clean = df.na.drop()

# Cast numeric columns to appropriate types and validate data
df_clean = df_clean.withColumn("sales", col("sales").cast("float")) \
                   .withColumn("transactions", col("transactions").cast("integer")) \
                   .withColumn("onpromotion", col("onpromotion").cast("float")) \
                   .withColumn("date", to_date(col("date"), "MM/dd/yyyy"))

# Register DataFrame as a temporary SQL table before cleaning for diagnostic queries
df_clean.createOrReplaceTempView("sales_data")

# Diagnostic queries to identify illogical data before cleaning
# 1. Count records with sales=0 and transactions>0
invalid_sales_transactions_query = """
SELECT COUNT(*) AS Invalid_Sales_Transactions
FROM sales_data
WHERE sales = 0 AND transactions > 0
"""
invalid_sales_transactions_count = spark.sql(invalid_sales_transactions_query).toPandas()['Invalid_Sales_Transactions'].iloc[0]
print(f"Number of records with sales=0 and transactions>0 before cleaning: {invalid_sales_transactions_count}")

# 2. Count records with sales>0 and transactions=0
invalid_transactions_sales_query = """
SELECT COUNT(*) AS Invalid_Transactions_Sales
FROM sales_data
WHERE sales > 0 AND transactions = 0
"""
invalid_transactions_sales_count = spark.sql(invalid_transactions_sales_query).toPandas()['Invalid_Transactions_Sales'].iloc[0]
print(f"Number of records with sales>0 and transactions=0 before cleaning: {invalid_transactions_sales_count}")

# 3. Count duplicate records
duplicate_records_query = """
SELECT store_nbr, family, date, sales, transactions, onpromotion, COUNT(*) as count
FROM sales_data
GROUP BY store_nbr, family, date, sales, transactions, onpromotion
HAVING count > 1
"""
duplicate_records = spark.sql(duplicate_records_query).toPandas()
duplicate_records_count = len(duplicate_records)
print(f"Number of duplicate records before cleaning: {duplicate_records_count}")

# Validate sales data: remove negative sales and records with sales=0 and transactions>0
df_clean = df_clean.filter(col("sales") >= 0)
# Ensure records with sales=0 and transactions>0 or sales>0 and transactions=0 are removed
df_clean = df_clean.filter(col("sales") > 0)  # Only keep records with sales > 0
df_clean = df_clean.filter(col("transactions") > 0)  # Only keep records with transactions > 0

# Remove duplicate records
df_clean = df_clean.dropDuplicates()

# Register cleaned DataFrame as a temporary SQL table
df_clean.createOrReplaceTempView("sales_data")

# Post-cleaning diagnostic to ensure no invalid records remain
post_cleaning_invalid_sales_query = """
SELECT COUNT(*) AS Remaining_Invalid_Sales_Transactions
FROM sales_data
WHERE sales = 0 AND transactions > 0
"""
remaining_invalid_sales_count = spark.sql(post_cleaning_invalid_sales_query).toPandas()['Remaining_Invalid_Sales_Transactions'].iloc[0]
print(f"Number of records with sales=0 and transactions>0 after cleaning: {remaining_invalid_sales_count}")

# Execute the 5 SQL queries for analysis
# Query 1: Top 5 high-sales product categories
query1 = """
SELECT family, 
       ROUND(SUM(sales), 2) AS Total_Sales, 
       ROUND(SUM(sales) * (1 - AVG(onpromotion) / 100), 2) AS Approx_Profit
FROM sales_data
WHERE sales IS NOT NULL
GROUP BY family
ORDER BY Approx_Profit DESC, Total_Sales DESC
LIMIT 5
"""
result1 = spark.sql(query1).toPandas()
result1.columns = ['Product Category', 'Total Sales', 'Approximate Profit']
result1['Total Sales'] = result1['Total Sales'].apply(lambda x: f"${int(x):,}")
result1['Approximate Profit'] = result1['Approximate Profit'].apply(lambda x: f"${int(x):,}")

# Query 2: City-wise sales with annual growth rate (remove LIMIT to include all cities)
query2 = """
WITH YearlySales AS (
  SELECT city, 
         YEAR(date) AS Year,
         ROUND(AVG(sales), 2) AS Avg_Sales
  FROM sales_data
  WHERE date IS NOT NULL
  GROUP BY city, YEAR(date)
)
SELECT city, 
       Avg_Sales,
       Year,
       ROUND(((Avg_Sales - LAG(Avg_Sales) OVER (PARTITION BY city ORDER BY Year)) / 
              LAG(Avg_Sales) OVER (PARTITION BY city ORDER BY Year)) * 100, 2) AS Growth_Rate_Percent
FROM YearlySales
WHERE Year IS NOT NULL
ORDER BY city, Year
"""
result2 = spark.sql(query2).toPandas()
result2.columns = ['City', 'Average Sales', 'Year', 'Annual Growth Rate (%)']
result2['Average Sales'] = result2['Average Sales'].apply(lambda x: f"${int(x):,}")
result2['Annual Growth Rate (%)'] = result2['Annual Growth Rate (%)'].apply(lambda x: f"{x:,.2f}%" if pd.notnull(x) else "N/A")

# Query 3: Customer purchase patterns
query3 = """
WITH CityTransactions AS (
  SELECT city, 
         COUNT(DISTINCT family) AS Unique_Categories,
         COUNT(DISTINCT transactions) AS Transaction_Count
  FROM sales_data
  WHERE sales IS NOT NULL
  GROUP BY city
  HAVING Unique_Categories > 2
)
SELECT Unique_Categories, 
       COUNT(city) AS City_Count,
       ROUND(AVG(Transaction_Count), 0) AS Avg_Transactions_Per_City
FROM CityTransactions
GROUP BY Unique_Categories
ORDER BY Unique_Categories
LIMIT 5
"""
result3 = spark.sql(query3).toPandas()
result3.columns = ['Number of Product Categories', 'Number of Cities', 'Average Transactions per City']
# Convert 'Average Transactions per City' to integer to remove decimal places
result3['Average Transactions per City'] = result3['Average Transactions per City'].astype(int)

# Query 4: Impact of promotions on sales
query4 = """
SELECT 
    CASE 
        WHEN onpromotion = 0 THEN 'No Promotion'
        WHEN onpromotion <= 10 THEN 'Low Promotion (0-10)'
        WHEN onpromotion <= 50 THEN 'Medium Promotion (11-50)'
        ELSE 'High Promotion (>50)'
    END AS Promotion_Range,
    ROUND(SUM(sales), 2) AS Total_Sales,
    ROUND(AVG(sales), 2) AS Avg_Sales
FROM sales_data
WHERE sales IS NOT NULL
GROUP BY Promotion_Range
ORDER BY Total_Sales DESC
"""
result4 = spark.sql(query4).toPandas()
result4.columns = ['Promotion Range', 'Total Sales', 'Average Sales']
result4['Total Sales'] = result4['Total Sales'].apply(lambda x: f"${int(x):,}")
result4['Average Sales'] = result4['Average Sales'].apply(lambda x: f"${int(x):,}")

# Query 5: High-risk orders
query5 = """
SELECT DISTINCT store_nbr,
       family,
       ROUND(SUM(sales), 2) AS Sales,
       SUM(transactions) AS Transactions
FROM sales_data
WHERE (sales < 10 OR transactions < 10) AND sales > 0 AND transactions > 0
GROUP BY store_nbr, family
ORDER BY Sales ASC, Transactions ASC
LIMIT 5
"""
result5 = spark.sql(query5).toPandas()
result5.columns = ['Store Number', 'Product Category', 'Sales', 'Transactions']
result5['Sales'] = result5['Sales'].apply(lambda x: f"${int(x):,}")

# Statistical Analysis: T-Test to compare sales between regions (cities)
# Collect sales data for two cities (e.g., first two cities in result2)
city_sales_query = """
SELECT city, ROUND(SUM(sales), 2) AS Total_Sales
FROM sales_data
WHERE city IS NOT NULL
GROUP BY city
ORDER BY Total_Sales DESC
"""
city_sales = spark.sql(city_sales_query).toPandas()

# Get top 10 cities by total sales for the dropdown filter
top_10_cities = city_sales['city'].head(10).tolist()
cities = sorted(top_10_cities)

# Perform T-Test between the top two cities
if len(city_sales) >= 2:
    top_cities = city_sales['city'].iloc[:2].tolist()
    city1_sales_query = f"""
    SELECT sales
    FROM sales_data
    WHERE city = '{top_cities[0]}' AND sales IS NOT NULL
    """
    city2_sales_query = f"""
    SELECT sales
    FROM sales_data
    WHERE city = '{top_cities[1]}' AND sales IS NOT NULL
    """
    city1_sales = spark.sql(city1_sales_query).toPandas()['sales'].values
    city2_sales = spark.sql(city2_sales_query).toPandas()['sales'].values

    # Perform T-Test
    t_stat, p_value = stats.ttest_ind(city1_sales, city2_sales, equal_var=False)
    t_test_result = f"T-Test between {top_cities[0]} and {top_cities[1]}: t-statistic = {t_stat:.4f}, p-value = {p_value:.4f}"
else:
    t_test_result = "Not enough cities to perform T-Test."

# Prepare data for Prediction (time series prediction, monthly aggregation) earlier to define time_range
sales_by_month_query = """
SELECT 
    YEAR(date) AS Year,
    MONTH(date) AS Month,
    store_nbr,
    family,
    ROUND(SUM(sales), 2) AS Total_Sales,
    ROUND(AVG(onpromotion), 2) AS Avg_Promotion
FROM sales_data
WHERE date IS NOT NULL
GROUP BY YEAR(date), MONTH(date), store_nbr, family
ORDER BY Year, Month, store_nbr, family
"""
sales_by_month = spark.sql(sales_by_month_query).toPandas()

# Create a time series
sales_by_month['ds'] = pd.to_datetime(sales_by_month['Year'].astype(str) + '-' + sales_by_month['Month'].astype(str) + '-01')
sales_by_month['y'] = sales_by_month['Total_Sales']

# Filter out rows where Total_Sales is 0 and replace with a small positive value
sales_by_month = sales_by_month[sales_by_month['y'] > 0]
sales_by_month['y'] = sales_by_month['y'].replace(0, 1)  # Replace any remaining zeros with 1

# Automatically detect the time range from the data
min_year = sales_by_month['Year'].min()
max_year = sales_by_month['Year'].max()
time_range = f"{min_year}-{max_year}"

# Create a graph of product categories based on co-purchased items using Spark SQL
G = nx.Graph()

# SQL query to generate category pairs and their total sales
query = """
WITH category_pairs AS (
  SELECT 
    t1.store_nbr,
    t1.family AS family1,
    t2.family AS family2,
    SUM(t1.sales) AS total_sales
  FROM sales_data t1
  JOIN sales_data t2
  ON t1.store_nbr = t2.store_nbr AND t1.date = t2.date AND t1.family < t2.family
  GROUP BY t1.store_nbr, t1.family, t2.family
)
SELECT family1, family2, SUM(total_sales) AS total_sales
FROM category_pairs
GROUP BY family1, family2
"""
edges_df = spark.sql(query)

# Collect edges and add to graph with a higher sales threshold
edges = edges_df.collect()
for row in edges:
    total_sales = row["total_sales"] if row["total_sales"] is not None else 1.0
    if total_sales > 20000000:  # Increased threshold to reduce edges
        G.add_edge(row["family1"], row["family2"], weight=total_sales)

# Run Eigenvector Centrality for Graph-Based Sales Impact metric
eigenvector_centrality = nx.eigenvector_centrality(G, weight="weight")

# Select top 100 nodes by Eigenvector Centrality score
top_nodes = sorted(eigenvector_centrality.items(), key=lambda x: x[1], reverse=True)[:100]
top_node_set = set(node for node, _ in top_nodes)

# Create a subgraph with top 100 nodes
subgraph = G.subgraph(top_node_set)

# Keep only the largest connected component
components = list(nx.connected_components(subgraph))
largest_component = max(components, key=len)
subgraph = subgraph.subgraph(largest_component)

# Get product category total sales for hover info
category_query = """
SELECT 
    family, 
    ROUND(SUM(sales), 2) AS Total_Sales
FROM sales_data
WHERE family IN ({})
GROUP BY family
""".format(",".join(f"'{node.replace('\'', '\'\'')}'" for node in subgraph.nodes()))
categories_df = spark.sql(category_query).collect()
category_dict = {row["family"]: row["Total_Sales"] for row in categories_df}

# Compute layout for visualization (spring_layout with adjusted k and iterations to reduce overlap)
pos = nx.spring_layout(subgraph, k=10.0, iterations=1000)

# Prepare data for Plotly visualization
edge_x = []
edge_y = []
for edge in subgraph.edges():
    x0, y0 = pos[edge[0]]
    x1, y1 = pos[edge[1]]
    edge_x.extend([x0, x1, None])
    edge_y.extend([y0, y1, None])

node_x = [pos[node][0] for node in subgraph.nodes()]
node_y = [pos[node][1] for node in subgraph.nodes()]

# Node sizes (reduced by 34% from 120 to 79)
node_sizes = [79 * eigenvector_centrality[node] for node in subgraph.nodes()]

# Use 4 colors for nodes and assign them based on size (largest to smallest)
colors = ["#FF5F4B", "#9191FF", "#51FF8C", "#FFF55F"]  # Red, Blue, Green, Yellow
# Sort nodes by Eigenvector Centrality (largest to smallest)
sorted_nodes = sorted(subgraph.nodes(), key=lambda node: eigenvector_centrality[node], reverse=True)
# Assign colors in order: Red, Blue, Green, Yellow (cycling through colors list)
node_colors = {node: colors[i % len(colors)] for i, node in enumerate(sorted_nodes)}
# Create node_colors list in the same order as subgraph.nodes()
node_colors_list = [node_colors[node] for node in subgraph.nodes()]

node_text = []
for node, color in zip(subgraph.nodes(), node_colors_list):
    bg_color = color
    text_color = "#000000"
    node_text.append(f"<span style='color:{text_color};background-color:{bg_color};padding:2px'>{node}<br>Total Sales ({time_range}): ${int(category_dict.get(node, 0)):,}<br>Eigenvector Centrality: {eigenvector_centrality[node]:.4f}</span>")

# Create Plotly figure for the graph
fig = Figure()

# Add edges with dark gray color and increased transparency
fig.add_trace(Scatter(
    x=edge_x, y=edge_y,
    line=dict(width=1.0, color="#212121", dash="solid"),
    hoverinfo="none",
    mode="lines",
    opacity=0.3
))

# Add nodes with enhanced styling
fig.add_trace(Scatter(
    x=node_x, y=node_y,
    mode="markers",
    marker=dict(
        size=node_sizes,
        color=node_colors_list,
        line=dict(width=1.0, color="#000000"),
        showscale=False
    ),
    text=node_text,
    hoverinfo="text"
))

# Update layout for professional look with autoscale
fig.update_layout(
    showlegend=False,
    hovermode="closest",
    margin=dict(b=20, l=17.5, r=17.5, t=20),
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    plot_bgcolor="#E5E5E5",
    paper_bgcolor="#E5E5E5",
    font=dict(family="Calibri", size=11),
    width=800,
    height=600
)

# Convert Plotly figure to HTML
graph_html = fig.to_html(full_html=False)

# Statistical Cards
total_sales = sales_by_month['Total_Sales'].sum()
avg_sales = sales_by_month['Total_Sales'].mean()
total_transactions_query = """
SELECT COUNT(transactions) AS Total_Transactions
FROM sales_data
"""
total_transactions = spark.sql(total_transactions_query).toPandas()['Total_Transactions'].iloc[0]
# Add separator to Total Transactions
total_transactions = f"{total_transactions:,}"

# Generate HTML for tables with improved styling and detailed English descriptions
# Add statistical cards to key_categories_graph.html
html_content = f"""
<html>
<head>
<style>
body {{
    font-family: Calibri, sans-serif;
    background-color: #f0f0f0;
    margin: 20px;
    color: #212121;
}}
h2 {{
    font-size: 22px;
    margin-top: 30px;
    margin-bottom: 5px;
    font-weight: bold;
}}
p {{
    font-size: 18px;  /* Increased from 16px */
    margin-bottom: 10px;
    color: #555;
    line-height: 1.5;
    text-align: justify;  /* Justify text */
}}
table {{
    border-collapse: collapse;
    width: 100%;
    max-width: 800px;
    margin-bottom: 30px;
    background-color: #ffffff;
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    border-radius: 8px;
    overflow: hidden;
}}
th, td {{
    padding: 12px 15px;
    text-align: left;
    border-bottom: 1px solid #e0e0e0;
}}
th {{
    background-color: #4A4A4A;
    color: #ffffff;
    font-size: 18px;
    font-weight: bold;
    text-transform: capitalize;
}}
td {{
    font-size: 16px;
    color: #333;
}}
td.number {{
    font-weight: bold;
}}
tr:nth-child(even) {{
    background-color: #E5E5E5;  /* Updated color */
}}
tr:hover {{
    background-color: #f2f2f2;
}}
.chart-container, .graph-container {{
    background-color: #ffffff;
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    border-radius: 8px;
    overflow: hidden;
    margin-bottom: 30px;
}}
.chart-container {{
    max-width: 1200px;
    margin-left: auto;
    margin-right: auto;
}}
.graph-container {{
    max-width: 800px;
    margin-left: auto;
    margin-right: auto;
}}
.chart-header, .graph-header {{
    background-color: #4A4A4A;
    color: #ffffff;
    font-size: 18px;
    font-weight: bold;
    padding: 12px 15px;
    text-align: left;
}}
#plotly-graph, #chart-train-test, #chart-sales-trend, #chart-sales-prediction, #chart-sales-by-city, #chart-correlation-heatmap, #card-total-sales, #card-avg-sales, #card-total-transactions {{
    opacity: 0;
    border-radius: 8px;
}}
.card-container {{
    display: flex;
    flex-wrap: nowrap;  /* Prevent wrapping to next line */
    gap: 10px;  /* Adjusted gap to ensure cards fit */
    justify-content: space-between;  /* Evenly space cards */
    margin-bottom: 20px;
}}
.card {{
    background-color: #ffffff;
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    border-radius: 8px;
    padding: 15px;
    width: 245px;  /* Adjusted width to fit cards in one row */
    text-align: center;
}}
.card-header {{
    background-color: #4A4A4A;
    color: #ffffff;
    font-size: 18px;  /* Increased to match table headers */
    font-weight: bold;
    padding: 10px;
    border-radius: 8px 8px 0 0;
    margin: -15px -15px 10px -15px;
}}
.card-value {{
    font-size: 18px;
    font-weight: bold;
    color: #333;
}}
.card-subtext {{
    font-size: 14px;  /* Increased from 12px */
    font-weight: bold;  /* Make text bold */
    color: #666;
    margin-top: 5px;
}}
#city-select {{
    padding: 5px;
    font-size: 16px;
    margin-bottom: 10px;
}}
label[for="city-select"] {{
    font-size: 22px;
    font-weight: bold;
    color: #FF5F4B;  /* Red color from graph */
}}
</style>
<script>
document.addEventListener("DOMContentLoaded", function() {{
    // Animate charts and graphs
    const elements = document.querySelectorAll("#plotly-graph, #chart-train-test, #chart-sales-trend, #chart-sales-prediction, #chart-sales-by-city, #chart-correlation-heatmap, #card-total-sales, #card-avg-sales, #card-total-transactions");
    elements.forEach(element => {{
        var duration = 5000; // 5 seconds
        var startTime = null;

        function animateElement(timestamp) {{
            if (!startTime) startTime = timestamp;
            var progress = (timestamp - startTime) / duration;
            if (progress > 1) progress = 1;
            element.style.opacity = progress;
            if (progress < 1) {{
                requestAnimationFrame(animateElement);
            }}
        }}
        requestAnimationFrame(animateElement);
    }});

    // Apply bold style to numeric columns
    const tables = document.querySelectorAll("table");
    tables.forEach(table => {{
        const headers = Array.from(table.querySelectorAll("th")).map(th => th.textContent.trim());
        const numericColumns = headers.map((header, index) => {{
            return [
                "Total Sales", 
                "Approximate Profit", 
                "Average Sales", 
                "Annual Growth Rate (%)", 
                "Number of Product Categories", 
                "Number of Cities", 
                "Average Transactions per City", 
                "Sales", 
                "Transactions"
            ].includes(header) ? index : -1;
        }}).filter(index => index !== -1);

        const rows = table.querySelectorAll("tr");
        rows.forEach(row => {{
            const cells = row.querySelectorAll("td");
            cells.forEach((cell, index) => {{
                if (numericColumns.includes(index)) {{
                    cell.classList.add("number");
                }}
            }});
        }});
    }});

    // Randomly select a city as default on page load
    const select = document.getElementById('city-select');
    const options = select.getElementsByTagName('option');
    if (options.length > 0) {{
        const randomIndex = Math.floor(Math.random() * options.length);
        select.selectedIndex = randomIndex;
        filterTable();
    }}
}});

function filterTable() {{
    const city = document.getElementById('city-select').value;
    const table = document.getElementById('table2');
    const rows = table.getElementsByTagName('tr');

    for (let i = 1; i < rows.length; i++) {{ // Start from 1 to skip header
        const cityCell = rows[i].getElementsByTagName('td')[0].textContent;
        if (cityCell === city) {{
            rows[i].style.display = '';
        }} else {{
            rows[i].style.display = 'none';
        }}
    }}
}}
</script>
</head>
<body>
<div style='max-width: 800px; margin: auto;'>
    <div class='card-container'>
        <div class='card'>
            <div class='card-header'>Total Sales</div>
            <div class='card-value' id='card-total-sales'>${int(total_sales):,}</div>
            <div class='card-subtext'>Sum of all sales across all transactions</div>
        </div>
        <div class='card'>
            <div class='card-header'>Average Sales per Transaction</div>
            <div class='card-value' id='card-avg-sales'>${int(avg_sales):,}</div>
            <div class='card-subtext'>Average sales per transaction</div>
        </div>
        <div class='card'>
            <div class='card-header'>Total Transactions</div>
            <div class='card-value' id='card-total-transactions'>{total_transactions}</div>
            <div class='card-subtext'>All Valid Records</div>  <!-- Updated text -->
        </div>
    </div>
    <div class='graph-container'>
        <div class='graph-header'>Interactive Graph of Key Product Categories</div>
        <div id='plotly-graph'>{graph_html}</div>
    </div>
    <h2>Top 5 High-Sales Product Categories</h2>
    <p>These are the top 5 product categories with the highest total sales and profit over the period {time_range}. For example, if a category like 'GROCERY I' has a Total Sales of $10M and an Approximate Profit of $9M, it means this category is highly profitable and you should consider increasing its inventory to maximize revenue.</p>
    {result1.to_html(index=False, classes="table", table_id="table1")}
    <h2>City-wise Sales with Annual Growth Rate</h2>
    <div>
        <label for="city-select">Select City: </label>
        <select id="city-select" onchange="filterTable()">
            {''.join(f'<option value="{city}">{city}</option>' for city in cities)}
        </select>
    </div>
    <p>This table shows the average annual sales in each city per year, along with the annual growth rate, over the period {time_range}. For instance, if a city has an Annual Growth Rate of 15% in 2015, it means sales increased by 15% compared to 2014, indicating a good opportunity for investment in that city.</p>
    {result2.to_html(index=False, classes="table", table_id="table2")}
    <h2>Customer Purchase Patterns</h2>
    <p>This table shows how many product categories are purchased in each city over the period {time_range}. For example, if 'Number of Product Categories' is 33, it means customers in 22 cities (Number of Cities) bought 33 different product categories, with an average of 310 transactions per city. This helps identify which cities buy a wide variety of products, useful for targeted marketing campaigns.</p>
    {result3.to_html(index=False, classes="table", table_id="table3")}
    <h2>Impact of Promotions on Sales</h2>
    <p>This table shows how different promotion levels affect sales over the period {time_range}. For example, if 'Medium Promotion (11-50)' has a Total Sales of $8M and an Average Sales of $500 per transaction, it means products with 11-50% discounts sell well, helping you decide the best discount range to boost sales.</p>
    {result4.to_html(index=False, classes="table", table_id="table4")}
    <h2>High-Risk Orders</h2>
    <p>This table lists orders with very low sales or transactions (less than 10) over the period {time_range}. For example, if a store has a Sales of $5 and only 3 Transactions for a product category, this order is risky and may lead to financial loss, so you might want to review these products.</p>
    {result5.to_html(index=False, classes="table", table_id="table5")}
</div>
</body>
</html>
"""

# Save the combined HTML for key categories graph
with open("C:/Projects/data/key_categories_graph.html", "w", encoding="utf-8") as f:
    f.write(html_content)
print("Interactive graph and query results saved to C:/Projects/data/key_categories_graph.html")

# Aggregate sales and promotions across all stores and categories for overall prediction
monthly_sales = sales_by_month.groupby('ds').agg({
    'y': 'sum',
    'Avg_Promotion': 'mean'
}).reset_index()
monthly_sales = monthly_sales.sort_values('ds')

# Set DatetimeIndex with explicit frequency for the dataframe
monthly_sales.set_index('ds', inplace=True)
monthly_sales.index = pd.date_range(start=monthly_sales.index[0], periods=len(monthly_sales), freq='MS')  # Explicitly set frequency
monthly_sales = monthly_sales.asfreq('MS')  # Ensure frequency is set

# Remove outliers (e.g., values above 95th percentile)
upper_limit = monthly_sales['y'].quantile(0.95)
monthly_sales = monthly_sales[monthly_sales['y'] <= upper_limit]

# Split data into train and test
train_size = int(len(monthly_sales) * 0.8)
train_data = monthly_sales.iloc[:train_size]
test_data = monthly_sales.iloc[train_size:]

# Plot train vs test data for visual comparison (raw values)
fig1 = Figure()
fig1.add_trace(Scatter(x=train_data.index, y=train_data['y'], mode='lines+markers', name='Train Data', line=dict(color='#9191FF', width=1.0, shape='spline'), marker=dict(size=3)))  # Blue
fig1.add_trace(Scatter(x=test_data.index, y=test_data['y'], mode='lines+markers', name='Test Data', line=dict(color='#FF5F4B', width=1.0, shape='spline'), marker=dict(size=3)))  # Red
fig1.update_layout(
    showlegend=True,
    hovermode="closest",
    margin=dict(b=20, l=17.5, r=17.5, t=20),
    xaxis=dict(
        showgrid=True,
        gridcolor="#E5E5E5",
        zeroline=False,
        showticklabels=True,
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title="Date",
        linecolor="#212121",
        linewidth=1
    ),
    yaxis=dict(
        showgrid=True,
        gridcolor="#E5E5E5",
        zeroline=False,
        showticklabels=True,
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title="Total Sales",
        linecolor="#212121",
        linewidth=2  # Double the thickness of y-axis
    ),
    legend=dict(
        font=dict(size=11, family="Calibri", color="#212121", weight='bold')
    ),
    plot_bgcolor="#E5E5E5",
    paper_bgcolor="#E5E5E5",
    font=dict(family="Calibri", size=11),
    width=750,
    height=375
)
train_test_html = fig1.to_html(full_html=False)

# Visualize the data to understand trends and seasonality
fig2 = Figure()
fig2.add_trace(Scatter(x=monthly_sales.index, y=monthly_sales['y'], mode='lines+markers', name='Total Sales', line=dict(color='#9191FF', width=1.0, shape='spline'), marker=dict(size=3)))  # Blue
fig2.update_layout(
    showlegend=True,
    hovermode="closest",
    margin=dict(b=20, l=17.5, r=17.5, t=20),
    xaxis=dict(
        showgrid=True,
        gridcolor="#E5E5E5",
        zeroline=False,
        showticklabels=True,
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title="Date",
        linecolor="#212121",
        linewidth=1
    ),
    yaxis=dict(
        showgrid=True,
        gridcolor="#E5E5E5",
        zeroline=False,
        showticklabels=True,
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title="Total Sales",
        linecolor="#212121",
        linewidth=2  # Double the thickness of y-axis
    ),
    legend=dict(
        font=dict(size=11, family="Calibri", color="#212121", weight='bold')
    ),
    plot_bgcolor="#E5E5E5",
    paper_bgcolor="#E5E5E5",
    font=dict(family="Calibri", size=11),
    width=750,
    height=375
)
sales_trend_html = fig2.to_html(full_html=False)

# Prepare features for XGBoost (only month and year)
monthly_sales['month'] = monthly_sales.index.month
monthly_sales['year'] = monthly_sales.index.year
X_xgb = monthly_sales[['month', 'year']]
y = monthly_sales['y']

# Split data into train and test for XGBoost
train_size = int(len(monthly_sales) * 0.8)
X_train_xgb = X_xgb.iloc[:train_size]
X_test_xgb = X_xgb.iloc[train_size:]
y_train_xgb = y.iloc[:train_size]
y_test_xgb = y.iloc[train_size:]

# Fit XGBoost model with initial parameters
xgb_model = XGBRegressor(
    n_estimators=100,
    learning_rate=0.1,
    max_depth=5,
    random_state=42
)
xgb_model.fit(X_train_xgb, y_train_xgb)

# Make predictions with XGBoost
train_predict_xgb = xgb_model.predict(X_train_xgb)
test_predict_xgb = xgb_model.predict(X_test_xgb)

# Evaluate the XGBoost model
train_rmse_xgb = np.sqrt(mean_squared_error(y_train_xgb, train_predict_xgb))
train_r2_xgb = r2_score(y_train_xgb, train_predict_xgb)
test_rmse_xgb = np.sqrt(mean_squared_error(y_test_xgb, test_predict_xgb))
test_r2_xgb = r2_score(y_test_xgb, test_predict_xgb)
print(f"XGBoost Train RMSE: {train_rmse_xgb:.4f}, Train R²: {train_r2_xgb:.4f}")
print(f"XGBoost Test RMSE: {test_rmse_xgb:.4f}, Test R²: {test_r2_xgb:.4f}")

# Save the XGBoost model
with open("C:/Projects/data/xgboost_model_step3.pkl", "wb") as f:
    pickle.dump(xgb_model, f)
print("XGBoost model saved to C:/Projects/data/xgboost_model_step3.pkl")

# Plot the results using Plotly for XGBoost
fig3 = Figure()
fig3.add_trace(Scatter(x=train_data.index, y=y_train_xgb, mode='lines+markers', name='Actual Train Sales', line=dict(color='#9191FF', width=1.0, shape='spline'), marker=dict(size=3)))  # Blue
fig3.add_trace(Scatter(x=train_data.index, y=train_predict_xgb, mode='lines+markers', name='Predicted Train Sales', line=dict(color='#FF5F4B', width=1.0, shape='spline'), marker=dict(size=3)))  # Red
fig3.add_trace(Scatter(x=test_data.index, y=y_test_xgb, mode='lines+markers', name='Actual Test Sales', line=dict(color='#51FF8C', width=1.0, shape='spline'), marker=dict(size=3)))  # Green
fig3.add_trace(Scatter(x=test_data.index, y=test_predict_xgb, mode='lines+markers', name='Predicted Test Sales', line=dict(color='#FFF55F', width=1.0, shape='spline'), marker=dict(size=3)))  # Yellow
fig3.update_layout(
    showlegend=True,
    hovermode="closest",
    margin=dict(b=20, l=17.5, r=17.5, t=20),
    xaxis=dict(
        showgrid=True,
        gridcolor="#E5E5E5",
        zeroline=False,
        showticklabels=True,
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title="Date",
        linecolor="#212121",
        linewidth=1
    ),
    yaxis=dict(
        showgrid=True,
        gridcolor="#E5E5E5",
        zeroline=False,
        showticklabels=True,
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title="Total Sales",
        linecolor="#212121",
        linewidth=2  # Double the thickness of y-axis
    ),
    legend=dict(
        font=dict(size=11, family="Calibri", color="#212121", weight='bold')
    ),
    plot_bgcolor="#E5E5E5",
    paper_bgcolor="#E5E5E5",
    font=dict(family="Calibri", size=11),
    width=750,
    height=375
)
sales_prediction_xgb_html = fig3.to_html(full_html=False)

# Additional Chart: Sales by City (Regional Sales)
city_sales_chart_query = """
SELECT city, ROUND(SUM(sales), 2) AS Total_Sales
FROM sales_data
WHERE city IS NOT NULL
GROUP BY city
ORDER BY Total_Sales DESC
LIMIT 10
"""
city_sales_chart = spark.sql(city_sales_chart_query).toPandas()

fig4 = Figure()
fig4.add_trace(Bar(
    x=city_sales_chart['Total_Sales'],
    y=city_sales_chart['city'],
    orientation='h',
    marker=dict(color='#9191FF'),
    text=city_sales_chart['Total_Sales'].apply(lambda x: f"${int(x):,}"),
    textposition='outside',  # Force text to be outside the bars
    textfont=dict(size=14, family="Calibri", color="#212121", weight='bold'),  # Larger and bold text
    width=0.536  # Reduced by 33%
))
fig4.update_layout(
    showlegend=False,
    hovermode="closest",
    margin=dict(b=20, l=17.5, r=17.5, t=20),
    xaxis=dict(
        showgrid=True,
        gridcolor="#E5E5E5",
        zeroline=False,
        showticklabels=True,
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title="Total Sales",
        linecolor="#212121",
        linewidth=1,
        range=[0, 200000000]  # Extend to 200M
    ),
    yaxis=dict(
        showgrid=True,
        gridcolor="#E5E5E5",
        zeroline=False,
        showticklabels=True,
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title="City",
        linecolor="#212121",
        linewidth=2  # Double the thickness of y-axis
    ),
    plot_bgcolor="#E5E5E5",
    paper_bgcolor="#E5E5E5",
    font=dict(family="Calibri", size=11),
    width=750,
    height=375
)
sales_by_city_html = fig4.to_html(full_html=False)

# Additional Chart: Correlation Heatmap
correlation_data = sales_by_month[['Total_Sales', 'Avg_Promotion']].corr()
fig5 = px.imshow(
    correlation_data,
    labels=dict(color="Correlation"),
    x=correlation_data.columns,
    y=correlation_data.index,
    color_continuous_scale=['#FFFFFF', '#51FF8C'],  # Changed to green
    text_auto=True
)
fig5.update_traces(
    textfont=dict(size=14, family="Calibri", color="#212121", weight='bold'),  # Larger and bold text
    colorbar=dict(
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        len=1,
        thickness=30  # Set width of the colorbar
    )
)
fig5.update_layout(
    margin=dict(b=20, l=17.5, r=17.5, t=20),
    xaxis=dict(
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        linecolor="#212121",
        linewidth=1
    ),
    yaxis=dict(
        tickfont=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        title_font=dict(size=13, family="Calibri", color="#212121", weight='bold'),
        linecolor="#212121",
        linewidth=2
    ),
    plot_bgcolor="#E5E5E5",
    paper_bgcolor="#E5E5E5",
    font=dict(family="Calibri", size=11),
    width=750,
    height=375  # Match the height of other charts
)
correlation_heatmap_html = fig5.to_html(full_html=False)

# Generate HTML for combined Charts page with XGBoost predictions and additional charts
# Statistical cards have been removed from here and moved to key_categories_graph.html
charts_content = f"""
<html>
<head>
<style>
body {{
    font-family: Calibri, sans-serif;
    background-color: #f0f0f0;
    margin: 20px;
    color: #212121;
}}
.chart-container {{
    background-color: #ffffff;
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    border-radius: 8px;
    overflow: hidden;
    width: 750px;
    margin: 0;
}}
.chart-empty {{
    background-color: #E5E5E5;  /* Updated color */
    width: 750px;
    height: 375px;
    border-radius: 8px;
}}
.chart-header {{
    background-color: #4A4A4A;
    color: #ffffff;
    font-size: 18px;
    font-weight: bold;
    padding: 12px 15px;
    text-align: left;
}}
#chart-train-test, #chart-sales-trend, #chart-sales-prediction, #chart-sales-by-city, #chart-correlation-heatmap {{
    opacity: 0;
    border-radius: 8px;
}}
.chart-row {{
    display: flex;
    flex-wrap: wrap;
    gap: 20px;
    justify-content: center;
    margin-bottom: 20px;
}}
</style>
<script>
document.addEventListener("DOMContentLoaded", function() {{
    // Animate charts
    const elements = document.querySelectorAll("#chart-train-test, #chart-sales-trend, #chart-sales-prediction, #chart-sales-by-city, #chart-correlation-heatmap");
    elements.forEach(element => {{
        var duration = 5000; // 5 seconds
        var startTime = null;

        function animateElement(timestamp) {{
            if (!startTime) startTime = timestamp;
            var progress = (timestamp - startTime) / duration;
            if (progress > 1) progress = 1;
            element.style.opacity = progress;
            if (progress < 1) {{
                requestAnimationFrame(animateElement);
            }}
        }}
        requestAnimationFrame(animateElement);
    }});
}});
</script>
</head>
<body>
<div style='max-width: 1520px; margin: auto;'>
    <div class='chart-row'>
        <div class='chart-container'>
            <div class='chart-header'>Train vs Test Data Comparison (Raw Values after Outlier Removal)</div>
            <div id='chart-train-test'>{train_test_html}</div>
        </div>
        <div class='chart-container'>
            <div class='chart-header'>Monthly Total Sales ({time_range}, Outliers Removed)</div>
            <div id='chart-sales-trend'>{sales_trend_html}</div>
        </div>
    </div>
    <div class='chart-row'>
        <div class='chart-container'>
            <div class='chart-header'>Monthly Sales Prediction with XGBoost (Raw Data, Outliers Removed)</div>
            <div id='chart-sales-prediction'>{sales_prediction_xgb_html}</div>
        </div>
        <div class='chart-container'>
            <div class='chart-header'>Top 10 Cities by Sales</div>
            <div id='chart-sales-by-city'>{sales_by_city_html}</div>
        </div>
    </div>
    <div class='chart-row'>
        <div class='chart-container'>
            <div class='chart-header'>Correlation Heatmap (Sales vs Promotions)</div>
            <div id='chart-correlation-heatmap'>{correlation_heatmap_html}</div>
        </div>
        <div class='chart-container'>
            <div class='chart-header'>Reserved for Future Chart</div>
            <div class='chart-empty'></div>
        </div>
    </div>
</div>
</body>
</html>
"""
# Save the combined Charts page
with open("C:/Projects/data/Chart.html", "w", encoding="utf-8") as f:
    f.write(charts_content)
print("Combined charts saved to C:/Projects/data/Chart.html")

# Add a delay to ensure files are fully written
print("Waiting for 2 seconds to ensure files are written...")
time.sleep(2)

# Check if files exist before opening
key_categories_path = os.path.abspath("C:/Projects/data/key_categories_graph.html")
charts_path = os.path.abspath("C:/Projects/data/Chart.html")

if os.path.exists(key_categories_path):
    print(f"File exists: {key_categories_path}")
    webbrowser.open(f"file://{key_categories_path}")
else:
    print(f"File does not exist: {key_categories_path}")

if os.path.exists(charts_path):
    print(f"File exists: {charts_path}")
    webbrowser.open(f"file://{charts_path}")
else:
    print(f"File does not exist: {charts_path}")

print("Attempted to open HTML files in the default web browser.")

# Generate PDF Report
class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'Sales Analysis Report', 0, 1, 'C')

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

pdf = PDF()
pdf.add_page()
pdf.set_font('Arial', '', 12)

# Add content to PDF
pdf.cell(0, 10, 'Sales Analysis Summary', 0, 1)
pdf.cell(0, 10, f'Total Sales ({time_range}): ${int(total_sales):,}', 0, 1)
pdf.cell(0, 10, f'Average Sales per Transaction ({time_range}): ${int(avg_sales):,}', 0, 1)
pdf.cell(0, 10, f'Total Transactions ({time_range}): {total_transactions}', 0, 1)
pdf.cell(0, 10, '', 0, 1)

# Add data cleaning summary
pdf.cell(0, 10, 'Data Cleaning Summary:', 0, 1)
pdf.cell(0, 10, f'Records with sales=0 and transactions>0 removed: {invalid_sales_transactions_count}', 0, 1)
pdf.cell(0, 10, f'Records with sales>0 and transactions=0 removed: {invalid_transactions_sales_count}', 0, 1)
pdf.cell(0, 10, f'Duplicate records removed: {duplicate_records_count}', 0, 1)
pdf.cell(0, 10, '', 0, 1)

pdf.cell(0, 10, 'XGBoost Prediction Results:', 0, 1)
pdf.cell(0, 10, f'Train RMSE: {train_rmse_xgb:.4f}, Train R²: {train_r2_xgb:.4f}', 0, 1)
pdf.cell(0, 10, f'Test RMSE: {test_rmse_xgb:.4f}, Test R²: {test_r2_xgb:.4f}', 0, 1)
pdf.cell(0, 10, '', 0, 1)

pdf.cell(0, 10, 'T-Test Results:', 0, 1)
pdf.cell(0, 10, t_test_result, 0, 1)
pdf.cell(0, 10, '', 0, 1)

pdf.cell(0, 10, 'Graph-Based Sales Impact (Eigenvector Centrality):', 0, 1)
top_product = max(eigenvector_centrality.items(), key=lambda x: x[1])
pdf.cell(0, 10, f'Top Product: {top_product[0]} with Centrality: {top_product[1]:.4f}', 0, 1)
pdf.cell(0, 10, '', 0, 1)

pdf.cell(0, 10, 'Recommendations:', 0, 1)
pdf.cell(0, 10, f'- Increase inventory for {top_product[0]} due to high sales impact.', 0, 1)
pdf.cell(0, 10, f'- Focus marketing efforts in {top_cities[0]} as it has significantly higher sales (T-Test p-value: {p_value:.4f}).', 0, 1)

pdf.output("C:/Projects/data/Sales_Analysis_Report.pdf")
print("PDF report saved to C:/Projects/data/Sales_Analysis_Report.pdf")

# Generate README for GitHub using raw string to avoid escape sequence issues
readme_content = r"""
# Sales Analysis Project

---

## Overview
The Sales Analysis Project is an advanced analytical tool designed to extract actionable insights from retail sales data, specifically tailored for large-scale retail environments. By identifying purchasing patterns, predicting future trends, and providing data-driven strategies, this project facilitates optimized inventory management, enhanced profitability, and informed decision-making for product ordering, discount strategies, and order management. The project has been rigorously tested for scalability, handling up to 500,000 records on a system with specifications including Microsoft Windows 11 Enterprise, a 14-core Intel i9-12900H processor, and 16 GB of RAM, delivering high performance while remaining adaptable to systems with lower specifications.

---

## Problem Statement
In the retail sector, extracting meaningful insights from large and complex sales datasets poses significant challenges. Traditional tools often lack the capability to handle high volumes of data efficiently, perform advanced graph-based analyses, or provide custom predictive models, leading to suboptimal decision-making in inventory management and promotional strategies. This project addresses these challenges by leveraging advanced data analytics, machine learning, and graph-based techniques to deliver comprehensive sales analysis, thereby facilitating targeted marketing strategies, efficient inventory allocation, and improved financial outcomes.

---

## Features and Execution Workflow

### 3.1 Data Loading and Preprocessing
Sales data is ingested using PySpark, ensuring efficient handling of large datasets. The preprocessing pipeline removes missing values, corrects data types (e.g., converting dates to a standard format), and filters out illogical entries such as negative sales or records with zero sales but non-zero transactions, ensuring data integrity for subsequent analyses.

### 3.2 Query-Based Analyses
Five targeted SQL queries are executed to uncover key insights:
- Identification of top-performing product categories by sales and profit.
- Analysis of city-wise sales with annual growth rates.
- Examination of customer purchasing patterns across cities.
- Evaluation of the impact of promotional discounts on sales.
- Detection of high-risk orders with low sales or transaction volumes.

### 3.3 Graph-Based Analysis
A co-purchase graph is constructed using NetworkX to model relationships between product categories, with edges weighted by total sales. Eigenvector Centrality is computed to identify influential products, providing strategic insights for inventory prioritization.

### 3.4 Statistical Analysis
A T-Test is performed to compare sales performance between regions, offering statistical evidence of disparities. The Graph-Based Sales Impact metric, derived from the co-purchase graph, further quantifies the influence of product categories within the sales network.

### 3.5 Machine Learning for Sales Prediction
An XGBoost model is trained to predict future sales based on temporal features (month and year). The model is evaluated using RMSE and R² metrics, ensuring robust predictive performance for business forecasting.

### 3.6 Visualization and Reporting
Interactive dashboards are generated using Plotly, featuring visualizations such as sales trends, prediction comparisons, and regional sales breakdowns. A detailed PDF report summarizes findings and provides actionable recommendations for business decision-making.

---

## Data Requirements
The project requires a dataset in CSV format with the following columns:
- `store_nbr`: Store identifier.
- `family`: Product category.
- `date`: Transaction date (format: MM/dd/yyyy).
- `sales`: Total sales amount (float).
- `transactions`: Number of transactions (integer).
- `onpromotion`: Promotion percentage (float).

The dataset should be placed at `C:/Projects/data/superstore_data_extended.csv` and is expected to contain at least 500,000 records to align with the project's scalability testing.

---

## Graph-Based Sales Impact Metric
The Graph-Based Sales Impact metric is derived using Eigenvector Centrality on a co-purchase graph of product categories. Let \( G = (V, E) \) represent the graph, where \( V \) denotes product categories and \( E \) represents co-purchases weighted by total sales. The adjacency matrix \( A \) is defined such that \( A_{ij} \) equals the total sales of co-purchases between categories \( i \) and \( j \). The Eigenvector Centrality \( x \) is computed as the eigenvector corresponding to the largest eigenvalue \( \lambda \) of \( A \), satisfying the equation:

\[
A x = \lambda x
\]

This metric identifies product categories with significant influence on the sales network. A high centrality score indicates a category that drives sales of related products, making it a critical focus for inventory optimization and marketing strategies.

---

## Dependencies
To run the project, ensure the following prerequisites are met:
- **Python**: Version 3.8 or 3.9.
- **PySpark**: Version 3.2.0 (for large-scale data processing and SQL query execution).
- **Pandas**: Version 1.5.3 (for data manipulation and analysis in tabular format).
- **Plotly**: Version 5.22.0 (for creating interactive visualizations and dashboards).
- **NetworkX**: Version 3.3 (for graph-based analysis and centrality computation).
- **Scikit-learn**: Version 1.5.0 (for data preprocessing and statistical analysis, e.g., T-Test).
- **XGBoost**: Version 2.0.3 (for sales prediction using the XGBoost model).
- **SciPy**: Version 1.13.1 (for statistical computations, e.g., T-Test).
- **FPDF**: Version 1.7.2 (for generating PDF reports).

Install the dependencies using:

```bash
pip install -r requirements.txt
```

---

## How to Run the Code

### 6.1 Prerequisites
- Ensure Python 3.8 or 3.9 is installed on your system.
- Verify that Java Development Kit (JDK) 8 is installed and configured.
- Place the `winutils.exe` file in `C:\hadoop\bin` for PySpark compatibility on Windows.
- Ensure an active internet connection for initial package installation.

### 6.2 Setup
1. Clone the repository:  
   ```bash
   git clone <repository-url>
   ```
2. Navigate to the project directory:  
   ```bash
   cd <project-directory>
   ```
3. Install dependencies:  
   ```bash
   pip install -r requirements.txt
   ```

### 6.3 Data Preparation
- Place the dataset file `superstore_data_extended.csv` in the directory `C:/Projects/data/`. Ensure the dataset contains at least 500,000 records for scalability testing, with the required columns as specified in the "Data Requirements" section.

### 6.4 Execution
- Run the script:  
  ```bash
  python sales_analysis.py
  ```
- The script will process the data, generate visualizations, and save outputs in `C:/Projects/data/`. Execution may take 1–2 minutes depending on system specifications and data size.

### 6.5 View Outputs
- Open the interactive dashboard at `C:/Projects/data/Chart.html` in a web browser.
- Review the detailed report at `C:/Projects/data/Sales_Analysis_Report.pdf`.

### 6.6 Troubleshooting
- **Memory Issues:** If you encounter memory errors with PySpark, adjust the configuration parameters in the script (e.g., increase `spark.driver.memory` to `10g` or higher).
- **File Not Found:** Ensure the dataset file is correctly placed at `C:/Projects/data/superstore_data_extended.csv` and the output directory has write permissions.
- **Visualization Issues:** If dashboards do not display, ensure your browser supports Plotly visualizations (e.g., use the latest version of Chrome or Firefox).

---

## Output Files
The project generates the following output files, which can be used for detailed analysis and reporting to stakeholders:

- **`C:/Projects/data/key_categories_graph.html`:** An interactive dashboard displaying a co-purchase graph of top product categories, statistical cards (e.g., total sales, average sales per transaction, total transactions), and query-based analysis results.
- **`C:/Projects/data/Chart.html`:** A comprehensive dashboard featuring visualizations such as train-test data comparison, monthly sales trends, XGBoost predictions, top cities by sales, and a correlation heatmap between sales and promotions.
- **`C:/Projects/data/Sales_Analysis_Report.pdf`:** A detailed PDF report summarizing key findings, including total sales, T-Test results, XGBoost prediction metrics, and actionable recommendations.
- **`C:/Projects/data/xgboost_model_step3.pkl`:** The trained XGBoost model for sales prediction, saved for future use.
- **`C:/Projects/data/README.md`:** This documentation file, providing an overview and instructions for the project.

---

## Interacting with the Dashboards
The project provides two interactive dashboards for exploring the analysis results. Users can interact with visualizations using hover effects and filters where applicable.

### Key Categories Graph Dashboard (`key_categories_graph.html`)
- **Statistical Cards:** Displays key metrics including total sales, average sales per transaction, and total transactions over the data period.
- **Co-purchase Graph:** An interactive graph of the top 100 product categories by Eigenvector Centrality, with node sizes reflecting centrality scores and edges weighted by co-purchase sales. Hover over nodes to view category details, total sales, and centrality scores.
- **Query Results:** Tables summarizing top product categories, city-wise sales growth (filterable by city via a dropdown), customer purchasing patterns, promotion impacts, and high-risk orders.

### Charts Dashboard (`Chart.html`)
- **Train vs Test Data Comparison:** Visualizes the split between training and test datasets, highlighting data distribution after outlier removal.
- **Monthly Total Sales Trend:** Displays aggregated sales trends over the data period, with outliers removed for clarity.
- **XGBoost Predictions:** Compares actual versus predicted sales for both training and test sets, showcasing the model's performance.
- **Top 10 Cities by Sales:** A bar chart of the top 10 cities by total sales, enabling regional performance analysis.
- **Correlation Heatmap:** Illustrates the correlation between sales and promotion levels, aiding in understanding promotional impacts.

---

## Contributing
Contributions to the Sales Analysis Project are welcome. To contribute:
1. Fork the repository and create a new branch for your feature or bug fix.
2. Ensure your code adheres to the project's coding standards and includes appropriate documentation.
3. Submit a pull request with a detailed description of your changes.

For major changes, please open an issue first to discuss the proposed modifications.

---

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

**Note:** Ensure that the output directory (`C:/Projects/data/`) exists and has write permissions before running the script. For large datasets, adjust Spark configuration parameters (e.g., `spark.driver.memory`) based on your system's resources to avoid memory issues.
"""
with open("C:/Projects/data/README.md", "w", encoding="utf-8") as f:
    f.write(readme_content)
print("README saved to C:/Projects/data/README.md")

# Stop Spark session
spark.stop()