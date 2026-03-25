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
    print("-"*5, "Step 2", "-"*5)
    
    # DF with only people from new york
    print("Filter only people from New York: ")
    df_ny = df.filter(df["city"] == "New York")
    df_ny.show()
    
    # Select only name and salary columns
    print("Select only name and salary columns: ")
    df_ny.select(df.name, df.salary).show()
    
    # Show how many people are from NY and what their avg salary is
    print("Number of people from NY: ", df_ny.count())
    print("Average salary from NY: ")
    df_ny.agg({"salary": "avg"}).show()

    
    spark.stop()

if __name__ == '__main__':
    main()