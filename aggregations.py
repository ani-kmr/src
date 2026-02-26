from pyspark.sql import functions as F

def compute_aggregations(joined_df):
    """
    
    Compute:
      1. category-wise total sales & units sold
      2. product-wise sales & units sold
      3. gender-wise total units sold
    """
    print("QUESTION 4:  Based on the description in the aggregation.py file  Write the pyspark code to perform the aggregations?")
    TODO:""
    
    

    category_sales = joined_df.groupBy("category").agg(
        F.sum("total_sale").alias("total_sales"),
        F.sum("quantity_sold").alias("total_quantity_sold")
    )

    product_sales = joined_df.groupBy("product_id", "name").agg(
        F.sum("total_sale").alias("total_sales"),
        F.sum("quantity_sold").alias("total_quantity_sold")
    )

    gender_stats = joined_df.groupBy("gender").agg(
        F.sum("quantity_sold").alias("total_quantity")
    )

    print("=== CATEGORY SALES ===")
    category_sales.show(truncate=False)

    print("=== PRODUCT SALES ===")
    product_sales.show(truncate=False)

    print("=== GENDER STATS ===")
    gender_stats.show(truncate=False)

    return category_sales, product_sales, gender_stats
    # pass
