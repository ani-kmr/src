def join_product_sales(products_df, sales_df):
    from pyspark.sql import functions as F
    print("QUESTION 3:  Ensure all sales records appear, even if product is missing from the product Datafrme and add the total_sale field (price*quantity_sold)?")
    TODO:""
    
    joined_df = sales_df.join(products_df, on="product_id", how="left")
    joined_df = joined_df.withColumn("total_sale", F.col("price") * F.col("quantity_sold"))
    joined_df.show(truncate=False)
    return joined_df
    # pass
