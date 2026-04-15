from config import create_spark_session
from data_loader import load_data
from processing import clean_reviews
from analysis import *

def main():
    spark = create_spark_session()
    orders, products, order_items, reviews, customers = load_data(spark)
    reviews = clean_reviews(reviews)

    basic_stats(orders, customers, order_items)
    orders_by_country(orders, customers)
    orders_by_time(orders)
    review_analysis(reviews)
    revenue_2024_by_category(orders, order_items, products)

    spark.stop()

if __name__ == "__main__":
    main()
