#!/bin/bash

echo "=== Bắt đầu chạy các bài tập Hadoop ==="
echo "Username: dinhnguyenanhthu"

# Định nghĩa đường dẫn ĐÚNG
HDFS_USER="/user/dinhnguyenanhthu"
INPUT_PATH="$HDFS_USER/movie/input"
OUTPUT_PATH="$HDFS_USER/movie/output"

# Tạo thư mục input nếu chưa có
echo "Tạo thư mục input trên HDFS..."
hdfs dfs -mkdir -p $INPUT_PATH

# Copy dữ liệu từ local lên HDFS (nếu chưa có)
echo "Copy dữ liệu lên HDFS..."
hdfs dfs -put -f input/* $INPUT_PATH/

# Kiểm tra dữ liệu đã được copy chưa
echo "Kiểm tra dữ liệu trên HDFS:"
hdfs dfs -ls $INPUT_PATH/

# Xóa output cũ nếu có
echo "Xóa output cũ..."
hdfs dfs -rm -r $OUTPUT_PATH 2>/dev/null

# Build project
echo "Build project..."
mvn clean package

if [ $? -ne 0 ]; then
    echo "Build thất bại!"
    exit 1
fi

echo "Build thành công!"

# Bài 1
echo "=== Chạy Bài 1: Movie Average Rating ==="
hadoop jar target/movie-ratings-analysis-1.0-SNAPSHOT.jar \
    bai1.MovieAverageRating \
    $INPUT_PATH/ratings_1.txt \
    $INPUT_PATH/movies.txt \
    $OUTPUT_PATH/bai1

# Kiểm tra kết quả Bài 1
echo "Kết quả Bài 1:"
hdfs dfs -ls $OUTPUT_PATH/bai1/
if hdfs dfs -test -e $OUTPUT_PATH/bai1/part-r-00000; then
    hdfs dfs -cat $OUTPUT_PATH/bai1/part-r-00000 | head -10
else
    echo "Không tìm thấy kết quả Bài 1!"
fi

# Bài 2
echo "=== Chạy Bài 2: Genre Analysis ==="
hadoop jar target/movie-ratings-analysis-1.0-SNAPSHOT.jar \
    bai2.GenreAnalysis \
    $INPUT_PATH/ratings_1.txt \
    $INPUT_PATH/movies.txt \
    $OUTPUT_PATH/bai2

echo "Kết quả Bài 2:"
hdfs dfs -ls $OUTPUT_PATH/bai2/
if hdfs dfs -test -e $OUTPUT_PATH/bai2/part-r-00000; then
    hdfs dfs -cat $OUTPUT_PATH/bai2/part-r-00000 | head -10
else
    echo "Không tìm thấy kết quả Bài 2!"
fi

# Bài 3
echo "=== Chạy Bài 3: Gender Analysis ==="
hadoop jar target/movie-ratings-analysis-1.0-SNAPSHOT.jar \
    bai3.GenderAnalysis \
    $INPUT_PATH/ratings_1.txt \
    $INPUT_PATH/users.txt \
    $INPUT_PATH/movies.txt \
    $OUTPUT_PATH/bai3

echo "Kết quả Bài 3:"
hdfs dfs -ls $OUTPUT_PATH/bai3/
if hdfs dfs -test -e $OUTPUT_PATH/bai3/part-r-00000; then
    hdfs dfs -cat $OUTPUT_PATH/bai3/part-r-00000 | head -10
else
    echo "Không tìm thấy kết quả Bài 3!"
fi

# Bài 4
echo "=== Chạy Bài 4: Age Group Analysis ==="
hadoop jar target/movie-ratings-analysis-1.0-SNAPSHOT.jar \
    bai4.AgeGroupAnalysis \
    $INPUT_PATH/ratings_1.txt \
    $INPUT_PATH/users.txt \
    $INPUT_PATH/movies.txt \
    $OUTPUT_PATH/bai4

echo "Kết quả Bài 4:"
hdfs dfs -ls $OUTPUT_PATH/bai4/
if hdfs dfs -test -e $OUTPUT_PATH/bai4/part-r-00000; then
    hdfs dfs -cat $OUTPUT_PATH/bai4/part-r-00000 | head -10
else
    echo "Không tìm thấy kết quả Bài 4!"
fi

# Lưu kết quả về local
echo "Lưu kết quả về local..."
mkdir -p output
hdfs dfs -get $OUTPUT_PATH/bai1 ./output/bai1_output 2>/dev/null
hdfs dfs -get $OUTPUT_PATH/bai2 ./output/bai2_output 2>/dev/null
hdfs dfs -get $OUTPUT_PATH/bai3 ./output/bai3_output 2>/dev/null
hdfs dfs -get $OUTPUT_PATH/bai4 ./output/bai4_output 2>/dev/null

echo "=== Hoàn thành! ==="
echo "Kết quả được lưu trong thư mục output/"
