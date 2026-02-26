from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from config.config import setup_session
import modules.clean_products as cp
import modules.clean_sales as cs
import modules.join_data as jd
import modules.aggregations as aggr
import modules.update_inventory as up


# setup_session()

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("SalesPipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    return spark


def main():
    spark = create_spark_session()
    print("#######################################################")
    # Products Data
    products_data = [
        (101, " Laptop ", 55000, 10, "Electronics"),
        (102, "Phone", -15000, 20, "Electronics"),
        (103, None, 500, 50, "Accessories"),
        (101, "Laptop", 55000, 10, "Electronics"),
        (104, "Headphones", 999999, 30, "Electronics"),
        (105, "Charger", 500, -3, "Accessories"),
        (106, " Tablet ", 25000, 15, "electronics"),
        (107, "Cables", 200, 20, "Accessories")
    ]
    product_cols = ["product_id", "name", "price", "quantity", "category"]
    products_df = spark.createDataFrame(products_data, product_cols)

    # Sales Data
    sales_data = [
        (1, 101, "Male", 2),
        (2, 106, "Female", 1),
        (3, 103, "Male", 5),
        (4, 104, "Female", 3),
        (5, 101, "Female", 1),
        (6, 106, "Male", 2),
        (7, 102, "Male", 4),
        (1, 107, "Male", 2)
    ]
    sales_cols = ["sale_id", "product_id", "gender", "quantity_sold"]
    sales_df = spark.createDataFrame(sales_data, sales_cols)

    # ---------------------------------------
    # CLEANING
    # ---------------------------------------

    products_df = cp.clean_products(products_df)
    sales_df = cs.clean_sales(sales_df)

    # ---------------------------------------
    # PROCESSING
    # ---------------------------------------

    joined_df = jd.join_product_sales(products_df, sales_df)

    # print("#############################################################")
    # print("=== Joined dataframe of product and sales ===")
    # joined_df.show(truncate=False)
    # compute_aggregations(joined_df)
    category_sales, product_sales, gender_stats = aggr.compute_aggregations(joined_df)

    updated_inventory = up.update_inventory(products_df, joined_df)
    print("#######################################################")


if __name__ == "__main__":
<<<<<<< HEAD
    main()
=======
    main()
>>>>>>> fdb66a3 (added new src code)
