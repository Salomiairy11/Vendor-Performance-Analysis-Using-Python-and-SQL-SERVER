# Vendor Performance Analytics Pipeline

## Project Overview

This project analyzes vendor performance using **SQL Server** and **Python** on a dataset of over 100,000 records spread across multiple transactional tables (purchases, sales, freight, and vendor invoices). The goal was to build an end-to-end analytics pipeline that cleans, integrates, and enriches raw data, then applies statistical analysis and business intelligence techniques to uncover inefficiencies in pricing, freight logistics, and inventory management.

## Tech Stack

- SQL Server → Data extraction and joins across multiple tables using CTEs.
- Python (Pandas, NumPy, SciPy, Matplotlib, Seaborn) → Data cleaning, KPI generation, EDA, statistical analysis, and visualization.
- Logging → Automated process tracking for reproducibility.
- Power BI → Dashboard for interactive reporting.

## Workflow

**1. Data Extraction (SQL + Pandas)**

- Merged purchases, sales, and freight tables into a single vendor-level dataset.
- Applied aggregations to compute purchase dollars, sales revenue, excise tax, and freight cost.
- Loaded results into Python using Pandas.

**2. Data Cleaning & KPI Engineering**

- Removed missing values, standardized text fields, and converted data types.
- Created new performance metrics:

  - Gross Profit = Sales – Purchases
  - Profit Margin (%) = Gross Profit ÷ Sales
  - Stock Turnover = Sales Quantity ÷ Purchase Quantity
  - Sales–Purchase Ratio = Sales Revenue ÷ Purchase Dollars
  - Unsold Inventory Value = (Purchased – Sold) × Purchase Price

**3. Exploratory & Statistical Analysis**

- Negative/Zero Value Detection → Identified products sold at a loss or never sold (obsolete stock).
- Outlier Detection → Found extreme freight costs and high-price premium products.
- Correlation Analysis →

  - Weak link between purchase price and sales → pricing not main driver of demand.
  - Strong correlation between purchase and sales quantities → efficient inventory flow.
  - Negative correlation between profit margin and sales price → competitive pricing pressure.

- Segmentation → Highlighted brands with low sales but high margins as candidates for promotional strategies.
- Confidence Intervals & Hypothesis Testing →

  - Low-sales vendors had higher but inconsistent profit margins.
  - High-sales vendors had stable but thinner margins.
  - T-tests confirmed significant differences in profit strategies across vendor groups.

**4. Business Insights & Recommendations**

- High-sales vendors → Could optimize profitability via pricing adjustments, bundling, or cost control.
- Low-sales vendors → Despite higher margins, they need marketing/distribution improvements.
- Freight → Large cost variations highlight the need for logistics optimization.
- Bulk Purchasing → Boxplots showed vendors buying in bulk secured lower unit prices, proving discount strategies effective.
- Inventory Risk → Unsold inventory value quantified capital locked in stock.

**5. Output & Reporting**

- Final cleaned summary stored back into SQL Server (`vendor_sales_summary`).
- Power BI dashboard built to visualize:

  - Top vendors/brands by sales
  - Vendor purchase contribution (%)
  - Stock turnover leaders and laggards
  - Pricing/margin trade-offs

## Key Results

- Created a single source of truth for vendor performance metrics.
- Quantified over \$361K in gross profit and identified inefficiencies in freight and stock turnover.
- Provided data-driven recommendations for vendor strategy, pricing, and inventory management.
