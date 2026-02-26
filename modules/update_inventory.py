from pyspark.sql import functions as F

def update_inventory(products_df, joined_df):
    
    print("QUESTION 5:  Update inventory with remaining quantity: remaining_quantity = quantity - sold_quantity  and display the results?")

    TODO:''
    

    sold_qty = joined_df.groupBy("product_id").agg(
        F.sum("quantity_sold").alias("sold_qty")
    )

    updated = (
        products_df.join(sold_qty, "product_id", "left")
        .withColumn(
            "remaining_quantity",
            F.col("quantity") - F.coalesce(F.col("sold_qty"), F.lit(0))
        )
    )
    print("=== UPDATED INVENTORY ===")
    updated.show(truncate=False)

    return updated
    # pass