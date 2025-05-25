# Sales Analysis Project

---

## Overview

The Sales Analysis Project is an advanced analytical tool designed to extract actionable insights from retail sales data, tailored for large-scale retail environments. It identifies purchasing patterns, predicts future trends, and provides data-driven strategies to optimize inventory management, enhance profitability, and inform decision-making for product ordering, discount strategies, and order management. The project is tested for scalability, handling up to 500,000 records on a system with Microsoft Windows 11 Enterprise, a 14-core Intel i9-12900H processor, and 16 GB of RAM, while remaining adaptable to lower-spec systems.

---

## Problem Statement

Extracting meaningful insights from large, complex retail sales datasets is challenging. Traditional tools often struggle with high data volumes, advanced graph-based analyses, or custom predictive models, leading to suboptimal inventory and promotional decisions. This project leverages advanced data analytics, machine learning, and graph-based techniques to deliver comprehensive sales analysis, enabling targeted marketing, efficient inventory allocation, and improved financial outcomes.

---

## Features and Execution Workflow

### Data Loading and Preprocessing

Sales data is ingested using PySpark for efficient handling of large datasets. The preprocessing pipeline removes missing values, standardizes data types (e.g., dates to MM/dd/yyyy), and filters illogical entries (e.g., negative sales or zero sales with non-zero transactions) to ensure data integrity.

### Query-Based Analyses

Five SQL queries uncover key insights:

- Top-performing product categories by sales and profit.
- City-wise sales with annual growth rates.
- Customer purchasing patterns across cities.
- Impact of promotional discounts on sales.
- High-risk orders with low sales or transaction volumes.

### Graph-Based Analysis

A co-purchase graph is built using NetworkX, modeling product category relationships with edges weighted by total sales. Eigenvector Centrality identifies influential products for inventory prioritization.

### Statistical Analysis

A T-Test compares sales performance between regions, providing statistical evidence of disparities. The Graph-Based Sales Impact metric, derived from the co-purchase graph, quantifies product category influence.

### Machine Learning for Sales Prediction

An XGBoost model predicts future sales using temporal features (month and year), evaluated with RMSE and R² metrics for robust forecasting.

### Visualization and Reporting

Interactive Plotly dashboards visualize sales trends, predictions, and regional breakdowns. A PDF report, generated with FPDF, summarizes findings and recommendations, opening automatically after execution.

---

## Data Requirements

The project requires a CSV dataset with:

- `store_nbr`: Store identifier.
- `family`: Product category.
- `date`: Transaction date (MM/dd/yyyy).
- `sales`: Total sales amount (float).
- `transactions`: Number of transactions (integer).
- `onpromotion`: Promotion percentage (float).

Place the dataset at `C:\sales_env\data\input\superstore_data_extended.csv`. It should contain at least 500,000 records for scalability testing.

---

## Graph-Based Sales Impact Metric

The Graph-Based Sales Impact metric uses Eigenvector Centrality on a co-purchase graph \( G = (V, E) \), where \( V \) is product categories and \( E \) is co-purchases weighted by sales. The adjacency matrix \( A \) has \( A\_{ij} \) as total co-purchase sales between categories \( i \) and \( j \). Eigenvector Centrality \( x \) is the eigenvector for the largest eigenvalue \( \lambda \):

\[
A x = \lambda x
\]

High centrality scores highlight categories driving related product sales, critical for inventory and marketing strategies.

---

## Dependencies

Prerequisites:

- **Python**: Version 3.11.9.
- **PySpark**: Version 3.2.0 (large-scale data processing).
- **Pandas**: Version 1.5.3 (data manipulation).
- **Plotly**: Version 5.22.0 (interactive visualizations).
- **NetworkX**: Version 3.3 (graph analysis).
- **Scikit-learn**: Version 1.5.0 (preprocessing, T-Test).
- **XGBoost**: Version 2.0.3 (sales prediction).
- **SciPy**: Version 1.13.1 (statistical computations).
- **FPDF**: Version 1.7.2 (PDF reports).

Install dependencies:

```bash
pip install -r C:\sales_env\requirements.txt
```

**Note**: If you encounter a "numpy.dtype size changed" error, install a compatible numpy version:

```bash
pip install numpy==1.24.3 --force-reinstall
```

---

## How to Run the Code

### Prerequisites

- Python 3.11.9 installed.
- Java Development Kit (JDK) 8 installed and configured.
- `winutils.exe` in `C:\hadoop\bin` for PySpark on Windows.
- Active internet connection for package installation.

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/VDeniz/SalesAnalysisProject.git
   ```
2. Install Python 3.11.9 from [https://www.python.org/downloads/release/python-3119/](https://www.python.org/downloads/release/python-3119/). Check "Add Python 3.11 to PATH".
3. Verify Python version:
   ```bash
   python --version
   ```
   Should show 3.11.9.
4. Install JDK 8 from [https://adoptopenjdk.net/?variant=openjdk8](https://adoptopenjdk.net/?variant=openjdk8) to `C:\Program Files\Java\jdk-8.0.452`.
   - Set JAVA_HOME: `setx JAVA_HOME "C:\Program Files\Java\jdk-8.0.452"`
   - Add to PATH: `setx PATH "%PATH%;C:\Program Files\Java\jdk-8.0.452\bin"`
   - Verify: `java -version` (should show 1.8.x).
5. Install Winutils for PySpark:
   - Create `C:\hadoop\bin`.
   - Download `winutils.exe` from [https://github.com/cdarlint/winutils/raw/master/hadoop-2.7.1/bin/winutils.exe](https://github.com/cdarlint/winutils/raw/master/hadoop-2.7.1/bin/winutils.exe) to `C:\hadoop\bin`.
   - Set HADOOP_HOME: `setx HADOOP_HOME "C:\hadoop"`
   - Add to PATH: `setx PATH "%PATH%;C:\hadoop\bin"`
6. Create virtual environment:
   ```bash
   python -m venv C:\sales_env
   ```
7. Activate virtual environment:
   ```bash
   C:\sales_env\Scripts\activate
   ```
8. Install dependencies:
   - Ensure `requirements.txt` is in `C:\sales_env` with:
     ```
     pyspark==3.2.0
     pandas==1.5.3
     plotly==5.22.0
     networkx==3.3
     scikit-learn==1.5.0
     xgboost==2.0.3
     scipy==1.13.1
     fpdf==1.7.2
     ```
   - Run:
     ```bash
     pip install -r C:\sales_env\requirements.txt
     ```

### Data Preparation

- Create directory structure:
  - `C:\sales_env`
  - `C:\sales_env\data`
  - `C:\sales_env\data\input`
  - `C:\sales_env\data\output`
- Download `superstore_data_extended.csv` from [Google Drive](https://drive.google.com/file/d/1tN2l1EbU4rwjJreilwWuooLiR8j96L-w/view?usp=sharing) and place it in `C:\sales_env\data\input`. Ensure it has at least 500,000 records with required columns (see Data Requirements).
- Copy `sales_analysis.py` to `C:\sales_env`.

### Execution

- Run the script:
  ```bash
  cd C:\sales_env
  python sales_analysis.py
  ```
- Execution takes 1–2 minutes depending on system specs and data size. The PDF report opens automatically after generation.

### View Outputs

- Outputs are in `C:\sales_env\data\output`:
  - `Sales_Analysis_Report.pdf`: Detailed report (auto-opens).
  - `xgboost_model_step3.pkl`: Trained XGBoost model.
  - `key_categories_graph.html`: Interactive co-purchase graph and query results.
  - `Chart.html`: Interactive dashboards.
- Download dashboards from Google Drive:
  - [Key Categories Graph Dashboard](https://drive.google.com/file/d/1z_ROTDisLcf7w6nHYwzV6DNLQczBspBH_2z/view?usp=sharing)
  - [Charts Dashboard](https://drive.google.com/file/d/1YjqrrpRcmdJ9DDzcMJkD4v0vmUGpB4XpN/view?usp=sharing)

### Troubleshooting

- **Memory Issues**: Adjust `spark.driver.memory` (e.g., to `10g`) in `sales_analysis.py` if PySpark memory errors occur.
- **File Not Found**: Ensure `superstore_data_extended.csv` is in `C:\sales_env\data\input` and `C:\sales_env\data\output` has write permissions.
- **Numpy Incompatibility**: Run `pip install numpy==1.24.3 --force-reinstall` if "numpy.dtype size changed" error appears.
- **Visualization Issues**: Use Chrome or Firefox for Plotly dashboards.

---

## Output Files

- **Dataset**: [superstore_data_extended.csv](https://drive.google.com/file/d/1tN2l1EbU4rwjJreilwWuooLiR8j96L-w/view?usp=sharing) - Sales data with 500,000+ records.
- **Interactive Dashboards**:
  - [key_categories_graph.html](https://drive.google.com/file/d/1z_ROTDisLcf7w6nHYwzJ6DNLQczBspBH_2z/view?usp=sharing) - Co-purchase graph, statistical cards, and query results.
  - [Chart.html](https://drive.google.com/file/d/1YjqrrpRcmdJ9DDzcMJDD4v0vmUGpB4XpN/view?usp=sharing) - Train-test comparison, sales trends, XGBoost predictions, top cities, and correlation heatmap.
- `C:\sales_env\data\output\Sales_Analysis_Report.pdf`: Detailed report.
- `C:\sales_env\data\output\xgboost_model_step3.pkl`: Trained XGBoost model.
- `C:\sales_env\data\output\key_categories_graph.html`: Co-purchase graph dashboard.
- `C:\sales_env\data\output\Chart.html`: Comprehensive dashboard.
- `C:\sales_env\README.md`: Project documentation.

---

## Interacting with the Dashboards

### Key Categories Graph Dashboard

- **Statistical Cards**: Total sales, average sales per transaction, and total transactions.
- **Co-purchase Graph**: Top 100 product categories by Eigenvector Centrality. Hover for category details, sales, and centrality scores.
- **Query Results**: Tables for top categories, city-wise sales growth (filterable by city), customer patterns, promotion impacts, and high-risk orders.

### Charts Dashboard

- **Train vs Test Data**: Shows data split post-outlier removal.
- **Monthly Sales Trend**: Aggregated sales over time, outlier-free.
- **XGBoost Predictions**: Actual vs predicted sales for train and test sets.
- **Top 10 Cities**: Bar chart of top cities by sales.
- **Correlation Heatmap**: Sales vs promotion correlations.

---

## Contributing

Fork the repository, create a branch, and submit a pull request with detailed changes. Open an issue for major modifications.

---

## License

MIT License. See [LICENSE](LICENSE) file.

---

**Note**: Ensure `C:\sales_env\data\output` exists with write permissions. Adjust Spark memory settings for large datasets to avoid issues.# Sales Analysis Project

---

## Overview

The Sales Analysis Project is an advanced analytical tool designed to extract actionable insights from retail sales data, specifically tailored for large-scale retail environments. By identifying purchasing patterns, predicting future trends, and providing data-driven strategies, this project facilitates optimized inventory management, enhanced profitability, and informed decision-making for product ordering, discount strategies, and order management. The project has been rigorously tested for scalability, handling up to 500,000 records on a system with specifications including Microsoft Windows 11 Enterprise, a 14-core Intel i9-12900H processor, and 16 GB of RAM, delivering high performance while remaining adaptable to systems with lower specifications.

---

## Problem Statement

In the retail sector, extracting meaningful insights from large and complex sales datasets poses significant challenges. Traditional tools often lack the capability to handle high volumes of data efficiently, perform advanced graph-based analyses, or provide custom predictive models, leading to suboptimal decision-making in inventory management and promotional strategies. This project addresses these challenges by leveraging advanced data analytics, machine learning, and graph-based techniques to deliver comprehensive sales analysis, thereby facilitating targeted marketing strategies, efficient inventory allocation, and improved financial outcomes.

---

## Features and Execution Workflow

### Data Loading and Preprocessing

Sales data is ingested using PySpark, ensuring efficient handling of large datasets. The preprocessing pipeline removes missing values, corrects data types (e.g., converting dates to a standard format), and filters out illogical entries such as negative sales or records with zero sales but non-zero transactions, ensuring data integrity for subsequent analyses.

### Query-Based Analyses

Five targeted SQL queries are executed to uncover key insights:

- Identification of top-performing product categories by sales and profit.
- Analysis of city-wise sales with annual growth rates.
- Examination of customer purchasing patterns across cities.
- Evaluation of the impact of promotional discounts on sales.
- Detection of high-risk orders with low sales or transaction volumes.

### Graph-Based Analysis

A co-purchase graph is constructed using NetworkX to model relationships between product categories, with edges weighted by total sales. Eigenvector Centrality is computed to identify influential products, providing strategic insights for inventory prioritization.

### Statistical Analysis

A T-Test is performed to compare sales performance between regions, offering statistical evidence of disparities. The Graph-Based Sales Impact metric, derived from the co-purchase graph, further quantifies the influence of product categories within the sales network.

### Machine Learning for Sales Prediction

An XGBoost model is trained to predict future sales based on temporal features (month and year). The model is evaluated using RMSE and R² metrics, ensuring robust predictive performance for business forecasting.

### Visualization and Reporting

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

The Graph-Based Sales Impact metric is derived using Eigenvector Centrality on a co-purchase graph of product categories. Let \( G = (V, E) \) represent the graph, where \( V \) denotes product categories and \( E \) represents co-purchases weighted by total sales. The adjacency matrix \( A \) is defined such that \( A\_{ij} \) equals the total sales of co-purchases between categories \( i \) and \( j \). The Eigenvector Centrality \( x \) is computed as the eigenvector corresponding to the largest eigenvalue \( \lambda \) of \( A \), satisfying the equation:

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

### Prerequisites

- Ensure Python 3.8 or 3.9 is installed on your system.
- Verify that Java Development Kit (JDK) 8 is installed and configured.
- Place the `winutils.exe` file in `C:\hadoop\bin` for PySpark compatibility on Windows.
- Ensure an active internet connection for initial package installation.

### Setup

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

### Data Preparation

- Download the dataset file `superstore_data_extended.csv` from the link provided in the "Output Files" section and place it in the directory `C:/Projects/data/`. Ensure the dataset contains at least 500,000 records for scalability testing, with the required columns as specified in the "Data Requirements" section.

### Execution

- Run the script:
  ```bash
  python sales_analysis.py
  ```
- The script will process the data, generate visualizations, and save outputs in `C:/Projects/data/`. Execution may take 1–2 minutes depending on system specifications and data size.

### View Outputs

- Download the interactive dashboards (see "Output Files" section) and open them in a web browser.
- Review the detailed report at `C:/Projects/data/Sales_Analysis_Report.pdf`.

### Troubleshooting

- **Memory Issues**: If you encounter memory errors with PySpark, adjust the configuration parameters in the script (e.g., increase `spark.driver.memory` to `10g` or higher).
- **File Not Found**: Ensure the dataset file is correctly placed at `C:/Projects/data/superstore_data_extended.csv` and the output directory has write permissions.
- **Visualization Issues**: If dashboards do not display, ensure your browser supports Plotly visualizations (e.g., use the latest version of Chrome or Firefox).

---

## Output Files

The project generates the following output files, which can be used for detailed analysis and reporting to stakeholders:

- **Dataset**: The dataset used for analysis is available on Google Drive: [Download superstore_data_extended.csv](https://drive.google.com/file/d/1tN2l1EbU4rwjJreilwWuooLiR8j96L-w/view?usp=sharing) - The dataset containing sales data with at least 500,000 records, required to run the analysis script.
- **Interactive Dashboards**: The following dashboards are available on Google Drive:
  - **Key Categories Graph Dashboard**: [Download key_categories_graph.html](https://drive.google.com/file/d/1z_ROTDisLcf7HYwzV6DNLQczBspBH_2z/view?usp=sharing) - An interactive dashboard displaying a co-purchase graph of top product categories, statistical cards (e.g., total sales, average sales per transaction, total transactions), and query-based analysis results.
  - **Charts Dashboard**: [Download Chart.html](https://drive.google.com/file/d/1YjqrrpRcmdJ9DDzcMJk4Av0vmUGp4XpN/view?usp=sharing) - A comprehensive dashboard featuring visualizations such as train-test data comparison, monthly sales trends, XGBoost predictions, top cities by sales, and a correlation heatmap between sales and promotions.
- **`C:/Projects/data/Sales_Analysis_Report.pdf`**: A detailed PDF report summarizing key findings, including total sales, T-Test results, XGBoost prediction metrics, and actionable recommendations.
- **`C:/Projects/data/xgboost_model_step3.pkl`**: The trained XGBoost model for sales prediction, saved for future use.
- **`C:/Projects/data/README.md`**: This documentation file, providing an overview and instructions for the project.

---

## Interacting with the Dashboards

The project provides two interactive dashboards for exploring the analysis results. Users can interact with visualizations using hover effects and filters where applicable.

### Key Categories Graph Dashboard

- **Statistical Cards**: Displays key metrics including total sales, average sales per transaction, and total transactions over the data period.
- **Co-purchase Graph**: An interactive graph of the top 100 product categories by Eigenvector Centrality, with node sizes reflecting centrality scores and edges weighted by co-purchase sales. Hover over nodes to view category details, total sales, and centrality scores.
- **Query Results**: Tables summarizing top product categories, city-wise sales growth (filterable by city via a dropdown), customer purchasing patterns, promotion impacts, and high-risk orders.

### Charts Dashboard

- **Train vs Test Data Comparison**: Visualizes the split between training and test datasets, highlighting data distribution after outlier removal.
- **Monthly Total Sales Trend**: Displays aggregated sales trends over the data period, with outliers removed for clarity.
- **XGBoost Predictions**: Compares actual versus predicted sales for both training and test sets, showcasing the model's performance.
- **Top 10 Cities by Sales**: A bar chart of the top 10 cities by total sales, enabling regional performance analysis.
- **Correlation Heatmap**: Illustrates the correlation between sales and promotion levels, aiding in understanding promotional impacts.

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

**Note**: Ensure that the output directory (`C:/Projects/data/`) exists and has write permissions before running the script. For large datasets, adjust Spark configuration parameters (e.g., `spark.driver.memory`) based on your system's resources to avoid memory issues.
