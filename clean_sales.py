from pyspark.sql import functions as F

def clean_sales(sales_df):
    
    print("QUESTION 2:  Clean sales dataset by ensuring:- quantity_sold > 0?")
    
    TODO:""
    
    cleaned_sales=sales_df.filter(F.col("quantity_sold") > 0)
    cleaned_sales.show(truncate=False)
    return cleaned_sales
    # pass
