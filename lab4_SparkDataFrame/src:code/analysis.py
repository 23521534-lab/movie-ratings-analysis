from pyspark.sql.functions import col, year, month, avg, sum as _sum, round

def basic_stats(orders, customers, order_items):
    total_orders = orders.select("Order_ID").distinct().count()
    total_customers = customers.select("Customer_Trx_ID").distinct().count()
    total_sellers = order_items.select("Seller_ID").distinct().count()

    print("Total Orders:", total_orders)
    print("Total Customers:", total_customers)
    print("Total Sellers:", total_sellers)

def orders_by_country(orders, customers):
    df = orders.join(customers, orders.Customer_Trx_ID == customers.Customer_Trx_ID)

    result = df.groupBy("Customer_Country") \
        .count() \
        .orderBy("count", ascending=False)

    result.show()

def orders_by_time(orders):
    df = orders.withColumn("year", year("Order_Purchase_Timestamp")) \
               .withColumn("month", month("Order_Purchase_Timestamp"))
    result = df.groupBy("year", "month").count() \
               .orderBy(["year", "month"], ascending=[True, False])
    result.show()

def review_analysis(reviews):
    print("Average Review Score:")
    reviews.select(round(avg("Review_Score"), 2).alias("Avg_Review_Score")).show()
    print("Count by Review Score:")
    reviews.groupBy("Review_Score").count().orderBy("Review_Score").show()

def revenue_2024_by_category(orders, order_items, products):
    df = orders.join(order_items, "Order_ID") \
               .join(products, "Product_ID")
    df_2024 = df.filter(year("Order_Purchase_Timestamp") == 2024)
    df_revenue = df_2024.withColumn("Revenue", col("Price") + col("Freight_Value"))
    result = df_revenue.groupBy("Product_Category_Name") \
        .agg(round(_sum("Revenue"), 2).alias("Total_Revenue")) \
        .orderBy("Total_Revenue", ascending=False)
    result.show()
