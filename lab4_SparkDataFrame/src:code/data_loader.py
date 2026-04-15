from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def load_data(spark):
    # Định nghĩa schema cho Order_Reviews để ép kiểu Review_Score là IntegerType
    review_schema = StructType([
        StructField("Review_ID", StringType(), True),
        StructField("Order_ID", StringType(), True),
        StructField("Review_Score", IntegerType(), True),
        StructField("Review_Comment_Title_En", StringType(), True),
        StructField("Review_Comment_Message_En", StringType(), True),
        StructField("Review_Creation_Date", StringType(), True),
        StructField("Review_Answer_Timestamp", StringType(), True)
    ])
    
    orders = spark.read.csv("Orders.csv", header=True, inferSchema=True, sep=";")
    products = spark.read.csv("Products.csv", header=True, inferSchema=True, sep=";")
    order_items = spark.read.csv("Order_Items.csv", header=True, inferSchema=True, sep=";")
    reviews = spark.read.csv("Order_Reviews.csv", header=True, schema=review_schema, sep=";")
    customers = spark.read.csv("Customer_List.csv", header=True, inferSchema=True, sep=";")

    return orders, products, order_items, reviews, customers
