from pyspark.sql.functions import col

def clean_reviews(reviews):
    # Lọc các giá trị Review_Score hợp lệ (1-5) và không NULL
    return reviews.filter(
        (col("Review_Score").isNotNull()) &
        (col("Review_Score") >= 1) &
        (col("Review_Score") <= 5)
    )
