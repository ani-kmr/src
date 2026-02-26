from pyspark.sql import functions as F

def clean_products(products_df):
    '''    
    Clean products dataset by:
      - Removing duplicates based on product_id
      - Filtering missing values for product_id, name, price i.e products should not contain any nulls
      - Ensuring price > 0 and quantity >= 0
      - Trimming + lowercasing name and category
      '''
    print("QUESTION 1:  Write PySpark clean and transformations to achieve all requirements for the product dataframe?")
    
    TODO:""


    df = products_df.dropDuplicates(["product_id"])
    df = df.filter(
        F.col("product_id").isNotNull() &
        F.col("name").isNotNull() &
        F.col("price").isNotNull()
    )

    df = df.filter((F.col("price") > 0) & (F.col("quantity") >= 0))
    df = df.withColumn("name", F.lower(F.trim("name")))
    df = df.withColumn("category", F.lower(F.trim("category")))
    df.show(truncate=False)

    return df
    # pass