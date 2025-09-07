import pandas as pd
import numpy as np
import logging
from create_connection import create_connection
from ingestion_db import ingest_db


logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    filemode="a"
)

logger = logging.getLogger('vendor_summary')

def get_vendor_summary(engine): 
    '''This funstion will merge the different tables to get the overall vendor summary'''
    
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT 
            VendorNumber, 
            SUM(Freight) AS TotalFreightCost
        FROM vendor_invoice 
        GROUP BY VendorNumber
        ),

    PurchaseSummary AS (
        SELECT 
            p.VendorNumber, 
            p.VendorName, 
            p.Brand, 
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity, 
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p 
        JOIN purchase_prices pp 
            ON p.Brand = pp.Brand 
        WHERE p.PurchasePrice > 0 
        GROUP BY p.VendorNumber, p.VendorName, p.Brand,p.Description,p.PurchasePrice,pp.Price,pp.Volume
        ),

    SalesSummary AS (
        SELECT 
            VendorNo, 
            Brand,
            SUM(SalesQuantity) as TotalSalesQuantity,
            SUM(SalesDollars) as TotalSalesDollars,
            SUM(ExciseTax) as TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity, 
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalExciseTax,
        fs.TotalFreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, 
    engine)
    return vendor_sales_summary

def clean_vendor_summary(vendor_sales_summary):
    '''This function will clean the vendor summary data and defines new columns'''
    vendor_sales_summary['Volume'] = vendor_sales_summary['Volume'].astype('float64')
    
    vendor_sales_summary.fillna(0, inplace = True)
    
    vendor_sales_summary['VendorName'] = vendor_sales_summary['VendorName'].str.strip()
    vendor_sales_summary['Description'] = vendor_sales_summary['Description'].str.strip()
    
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit'] /vendor_sales_summary['TotalSalesDollars'])*100
    vendor_sales_summary['StockTurnOver'] = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']
    
    float_cols = [
    "PurchasePrice", "ActualPrice", "Volume", "TotalPurchaseDollars",
    "TotalSalesQuantity","TotalSalesDollars", "TotalExciseTax", "TotalFreightCost",
    "GrossProfit", "ProfitMargin", "StockTurnOver", "SalesPurchaseRatio"
    ]
    vendor_sales_summary[float_cols] = vendor_sales_summary[float_cols].replace([np.inf, -np.inf], np.nan)
    vendor_sales_summary[float_cols] = vendor_sales_summary[float_cols].fillna(0)
    return vendor_sales_summary

if __name__ == "__main__":
    engine = create_connection()
    
    logger.info("Creating Vendor Summary Table....")
    summary_df = get_vendor_summary(engine)
    logger.info(summary_df.head())
    
    logger.info("Cleaning Vendor Summary Table....")
    cleaned_summary_df = clean_vendor_summary(summary_df)
    logger.info(cleaned_summary_df.head())
    
    logger.info("Ingesting Vendor Summary Table to Database....")
    ingest_db(cleaned_summary_df, 'vendor_sales_summary', engine)
    logger.info("Vendor Summary Table Ingested Successfully!")
    