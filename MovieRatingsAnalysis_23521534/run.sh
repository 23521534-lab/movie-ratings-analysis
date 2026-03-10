#!/bin/bash

# Tạo thư mục input
hdfs dfs -mkdir -p /user/student/movie/input

# Upload dữ liệu
hdfs dfs -put -f input/* /user/student/movie/input/

# Xoá output cũ
hdfs dfs -rm -r -f /user/student/movie/output

# Build project
mvn clean package

# Bài 1
hadoop jar target/movie-ratings-analysis-1.0-SNAPSHOT.jar \
bai1.MovieAverageRating \
/user/student/movie/input/ratings_1.txt \
/user/student/movie/input/movies.txt \
/user/student/movie/output/bai1

# Bài 2
hadoop jar target/movie-ratings-analysis-1.0-SNAPSHOT.jar \
bai2.GenreAnalysis \
/user/student/movie/input/ratings_1.txt \
/user/student/movie/input/movies.txt \
/user/student/movie/output/bai2

# Bài 3
hadoop jar target/movie-ratings-analysis-1.0-SNAPSHOT.jar \
bai3.GenderAnalysis \
/user/student/movie/input/ratings_1.txt \
/user/student/movie/input/users.txt \
/user/student/movie/input/movies.txt \
/user/student/movie/output/bai3

# Bài 4
hadoop jar target/movie-ratings-analysis-1.0-SNAPSHOT.jar \
bai4.AgeGroupAnalysis \
/user/student/movie/input/ratings_1.txt \
/user/student/movie/input/users.txt \
/user/student/movie/input/movies.txt \
/user/student/movie/output/bai4

echo "=== Bài 1 ==="
hdfs dfs -cat /user/student/movie/output/bai1/part-r-00000 | head -5

echo "=== Bài 2 ==="
hdfs dfs -cat /user/student/movie/output/bai2/part-r-00000

echo "=== Bài 3 ==="
hdfs dfs -cat /user/student/movie/output/bai3/part-r-00000 | head -5

echo "=== Bài 4 ==="
hdfs dfs -cat /user/student/movie/output/bai4/part-r-00000 | head -5