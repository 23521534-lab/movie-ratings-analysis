#!/bin/bash
# ============================================================
# HƯỚNG DẪN CHẠY TOÀN BỘ - HADOOP JAVA - HOTEL REVIEW
# Chạy từng khối lệnh theo thứ tự
# ============================================================

# ============================================================
# BƯỚC 0: BIẾN MÔI TRƯỜNG (chạy 1 lần)
# ============================================================
export HADOOP_HOME=/opt/homebrew/Cellar/hadoop/3.4.3/libexec
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

# ============================================================
# BƯỚC 1: UPLOAD DỮ LIỆU LÊN HDFS
# ============================================================
# Tạo thư mục
hdfs dfs -mkdir -p /user/hadoop/hotel-review/input

# Upload file
hdfs dfs -put hotel-review.csv /user/hadoop/hotel-review/input/
hdfs dfs -put stopwords.txt    /user/hadoop/hotel-review/input/

# Kiểm tra
hdfs dfs -ls /user/hadoop/hotel-review/input/

# ============================================================
# BƯỚC 2: BIÊN DỊCH JAVA
# ============================================================
mkdir -p ~/hotel_java/classes

# Compile tất cả file Java cùng lúc
javac -encoding UTF-8 \
  -classpath "$HADOOP_CLASSPATH" \
  -d ~/hotel_java/classes \
  ~/hotel_java/src/main/java/hotel/Bai1Preprocessing.java \
  ~/hotel_java/src/main/java/hotel/Bai2aWordCount.java \
  ~/hotel_java/src/main/java/hotel/Bai2bcStats.java \
  ~/hotel_java/src/main/java/hotel/Bai3AspectSentiment.java \
  ~/hotel_java/src/main/java/hotel/Bai4SentimentWords.java \
  ~/hotel_java/src/main/java/hotel/Bai5RelevantWords.java

# Đóng gói thành JAR
jar -cvf ~/hotel_java/hotel-review.jar -C ~/hotel_java/classes .

# Kiểm tra JAR đã tạo thành công
ls -lh ~/hotel_java/hotel-review.jar

# ============================================================
# BƯỚC 3: ĐẶT BIẾN JAR (dùng cho các bước sau)
# ============================================================
JAR=~/hotel_java/hotel-review.jar
HDFS_INPUT=/user/hadoop/hotel-review/input/hotel-review.csv
STOPWORDS_PATH=hdfs:///user/hadoop/hotel-review/input/stopwords.txt
OUTPUT_BASE=/user/hadoop/hotel-review/output

# ============================================================
# BÀI 1: TIỀN XỬ LÝ (lowercase + tách từ + lọc stopwords)
# ============================================================
# Xóa output cũ nếu có
hdfs dfs -rm -r $OUTPUT_BASE/bai1 2>/dev/null

hadoop jar $JAR hotel.Bai1Preprocessing \
  $HDFS_INPUT \
  $OUTPUT_BASE/bai1 \
  $STOPWORDS_PATH

# Xem kết quả mẫu (5 dòng đầu)
echo "=== KẾT QUẢ BÀI 1 (5 dòng đầu) ==="
hdfs dfs -cat $OUTPUT_BASE/bai1/part-r-00000 | head -5

# ============================================================
# BÀI 2a: TOP 5 TỪ PHỔ BIẾN NHẤT
# ============================================================
hdfs dfs -rm -r $OUTPUT_BASE/bai2a 2>/dev/null

hadoop jar $JAR hotel.Bai2aWordCount \
  $OUTPUT_BASE/bai1 \
  $OUTPUT_BASE/bai2a

echo "=== KẾT QUẢ BÀI 2a: TOP 5 TỪ PHỔ BIẾN NHẤT ==="
hdfs dfs -cat $OUTPUT_BASE/bai2a/part-r-00000 \
  | sort -t$'\t' -k2 -nr \
  | head -5

# ============================================================
# BÀI 2b & 2c: THỐNG KÊ CATEGORY VÀ ASPECT
# ============================================================
hdfs dfs -rm -r $OUTPUT_BASE/bai2bc 2>/dev/null

hadoop jar $JAR hotel.Bai2bcStats \
  $OUTPUT_BASE/bai1 \
  $OUTPUT_BASE/bai2bc

echo "=== BÀI 2b: THỐNG KÊ THEO CATEGORY ==="
hdfs dfs -cat $OUTPUT_BASE/bai2bc/part-r-00000 \
  | grep "^CATEGORY:" \
  | sort -t$'\t' -k2 -nr

echo "=== BÀI 2c: THỐNG KÊ THEO ASPECT ==="
hdfs dfs -cat $OUTPUT_BASE/bai2bc/part-r-00000 \
  | grep "^ASPECT:" \
  | sort -t$'\t' -k2 -nr

# ============================================================
# BÀI 3: ASPECT NHIỀU POSITIVE / NEGATIVE NHẤT
# ============================================================
hdfs dfs -rm -r $OUTPUT_BASE/bai3 2>/dev/null

hadoop jar $JAR hotel.Bai3AspectSentiment \
  $OUTPUT_BASE/bai1 \
  $OUTPUT_BASE/bai3

echo "=== BÀI 3: ASPECT NHIỀU POSITIVE NHẤT ==="
hdfs dfs -cat $OUTPUT_BASE/bai3/part-r-00000 \
  | grep "positive" \
  | sort -t$'\t' -k3 -nr \
  | head -3

echo "=== BÀI 3: ASPECT NHIỀU NEGATIVE NHẤT ==="
hdfs dfs -cat $OUTPUT_BASE/bai3/part-r-00000 \
  | grep "negative" \
  | sort -t$'\t' -k3 -nr \
  | head -3

# ============================================================
# BÀI 4: TOP 5 TỪ TÍCH CỰC / TIÊU CỰC THEO CATEGORY
# ============================================================
hdfs dfs -rm -r $OUTPUT_BASE/bai4 2>/dev/null

hadoop jar $JAR hotel.Bai4SentimentWords \
  $OUTPUT_BASE/bai1 \
  $OUTPUT_BASE/bai4

echo "=== BÀI 4: TOP 5 TỪ POSITIVE (ví dụ category HOTEL) ==="
hdfs dfs -cat $OUTPUT_BASE/bai4/part-r-00000 \
  | grep "^HOTEL|positive|" \
  | sort -t$'\t' -k2 -nr \
  | head -5

echo "=== BÀI 4: TOP 5 TỪ NEGATIVE (ví dụ category HOTEL) ==="
hdfs dfs -cat $OUTPUT_BASE/bai4/part-r-00000 \
  | grep "^HOTEL|negative|" \
  | sort -t$'\t' -k2 -nr \
  | head -5

# Xem tất cả categories
for cat in HOTEL ROOMS SERVICE "FOOD&DRINKS" LOCATION; do
  echo "--- $cat POSITIVE ---"
  hdfs dfs -cat $OUTPUT_BASE/bai4/part-r-00000 \
    | grep "^${cat}|positive|" | sort -t$'\t' -k2 -nr | head -5
  echo "--- $cat NEGATIVE ---"
  hdfs dfs -cat $OUTPUT_BASE/bai4/part-r-00000 \
    | grep "^${cat}|negative|" | sort -t$'\t' -k2 -nr | head -5
done

# ============================================================
# BÀI 5: TOP 5 TỪ LIÊN QUAN NHẤT THEO CATEGORY
# ============================================================
hdfs dfs -rm -r $OUTPUT_BASE/bai5 2>/dev/null

hadoop jar $JAR hotel.Bai5RelevantWords \
  $OUTPUT_BASE/bai1 \
  $OUTPUT_BASE/bai5

echo "=== BÀI 5: TOP 5 TỪ LIÊN QUAN THEO TỪNG CATEGORY ==="
for cat in HOTEL ROOMS SERVICE "FOOD&DRINKS" LOCATION "ROOM_AMENITIES"; do
  echo "--- $cat ---"
  hdfs dfs -cat $OUTPUT_BASE/bai5/part-r-00000 \
    | grep "^${cat}|" \
    | sort -t$'\t' -k2 -nr \
    | head -5
done

# ============================================================
# DOWNLOAD KẾT QUẢ VỀ MÁY (tùy chọn)
# ============================================================
mkdir -p ~/hotel_results
hdfs dfs -get $OUTPUT_BASE/bai1/part-r-00000   ~/hotel_results/bai1.txt
hdfs dfs -get $OUTPUT_BASE/bai2a/part-r-00000  ~/hotel_results/bai2a_wordcount.txt
hdfs dfs -get $OUTPUT_BASE/bai2bc/part-r-00000 ~/hotel_results/bai2bc_stats.txt
hdfs dfs -get $OUTPUT_BASE/bai3/part-r-00000   ~/hotel_results/bai3_sentiment.txt
hdfs dfs -get $OUTPUT_BASE/bai4/part-r-00000   ~/hotel_results/bai4_words.txt
hdfs dfs -get $OUTPUT_BASE/bai5/part-r-00000   ~/hotel_results/bai5_relevant.txt
echo "Đã download kết quả về ~/hotel_results/"
