# Author: Nick Laustrup
# Date: 3/25/2026
# PySpark DataFrames
from pyspark.sql import SparkSession, functions as F

def main():
    print("=" * 100)
    spark = SparkSession.builder.appName("DataFrames").getOrCreate()
    # ----------------------------------- #
    # Step 1: Load and Explore Data
    # ----------------------------------- #
    print("-"*5, "Step 1", "-"*5)
    
    # Load data
    df = spark.read.csv("people.csv", header=True, inferSchema=True)
    
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

    # ----------------------------------- #
    # Step 3: Grouping and Aggregation
    # ----------------------------------- #
    print("-"*5, "Step 3", "-"*5)
    
    # Group the data by city and calculate the average salary per city
    print("Group by city & Sort by Avg Salary: ")
    df_city = df.groupBy("city").avg("salary").sort("avg(salary)", ascending=False)
    df_city.show()
    
    # ----------------------------------- #
    # Step 4: DataFrame Transformation
    # ----------------------------------- #
    print("-"*5, "Step 4", "-"*5)
        
    # Add new column: income bracket based on salary
    df_with_bracket = df.withColumn(
        "income_bracket",
        F.when(F.col("salary") < 80000, "Low")
        .when(F.col("salary") < 100000, "Mid")
        .otherwise("High")
    )
    
    print("Add income bracket: ")
    df_with_bracket.show()

    print("Number of people in each income bracket: ")
    df_income_count = df_with_bracket.groupBy(df_with_bracket.income_bracket).count()
    df_income_count.show()
    
    print("=" * 100)
    spark.stop()

if __name__ == '__main__':
    main()