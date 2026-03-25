# Author: Nick Laustrup
# Date: 3/25/2026
# PySpark DataFrames
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("DataFrames").getOrCreate()
    
    # ----------------------------------- #
    # Step 1: Load and Explore Data
    # ----------------------------------- #
    print("-"*5, "Step 1", "-"*5)
    # Load data
    df = spark.read.csv("people.csv", header=True, inferSchema=True)
    print("=" * 100)
    
    # Show first few rows
    print("Printing first few rows: ")
    df.show(n=5)
    
    # Print schema
    print("Printing schema: ")
    df.printSchema()
    
    # Count how many records
    print("Number of records in dataset: ", df.count())
    
    # ----------------------------------- #
    # Step 2: Filtering and Selecting Columns
    # ----------------------------------- #
    print("-"*5, "Step 1", "-"*5)
    
    spark.stop()

if __name__ == '__main__':
    main()