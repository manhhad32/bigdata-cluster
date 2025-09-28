package com.bigdata;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Chương trình Spark đơn giản để đếm tần suất xuất hiện của tất cả các từ
 * trong một file văn bản.
 */
public class WordCount {

    // Sử dụng Pattern để tách từ, giúp xử lý các khoảng trắng linh hoạt hơn
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        // --- KHỞI TẠO ---
        // Khởi tạo SparkSession, là điểm bắt đầu của mọi chức năng Spark
        SparkSession spark = SparkSession.builder()
                .appName("JavaWordCount") // Đổi tên ứng dụng cho phù hợp
                .getOrCreate();

        // --- BƯỚC 1: ĐỌC DỮ LIỆU ĐẦU VÀO ---
        // Đọc file văn bản chính từ HDFS. Mỗi dòng trong file sẽ trở thành một phần tử trong RDD.
        String mainTextPath = "hdfs://namenode:9000/data/kosarak25k.txt";
        JavaRDD<String> lines = spark.read().textFile(mainTextPath).javaRDD();

        // --- BƯỚC 2: XỬ LÝ VÀ ĐẾM TỪ (TRANSFORMATION) ---
        // Chuỗi các biến đổi RDD để thực hiện việc đếm từ
        JavaPairRDD<String, Integer> counts = lines
                // Tách mỗi dòng thành các từ riêng lẻ
                .flatMap(line -> Arrays.asList(SPACE.split(line)).iterator())
                // Lọc ra các từ rỗng có thể được tạo ra do nhiều khoảng trắng liền nhau
                .filter(word -> !word.isEmpty())
                // Ánh xạ mỗi từ thành một cặp key-value (từ, 1) - BƯỚC MAP
                .mapToPair(word -> new Tuple2<>(word, 1))
                // Tổng hợp số đếm cho mỗi từ - BƯỚC REDUCE
                .reduceByKey(Integer::sum); // (a, b) -> a + b

        // --- BƯỚC 3: LƯU KẾT QUẢ (ACTION) ---

        // In ra 20 kết quả đầu tiên để kiểm tra nhanh. `take()` là một action.
        List<Tuple2<String, Integer>> resultsSample = counts.take(20);
        System.out.println("--- XEM TRUOC 20 KET QUA DAU TIEN ---");
        for (Tuple2<String, Integer> tuple : resultsSample) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        System.out.println("---------------------------------------");

        // Ghi kết quả RDD ra HDFS. Đây là một action khác.
        String outputPath = "hdfs://namenode:9000/data/wordcount_output_java";
        counts.saveAsTextFile(outputPath);
        
        System.out.println("Hoan thanh dem tu! Kiem tra ket qua tai thu muc " + outputPath + " tren HDFS.");

        // Dừng SparkSession để giải phóng tài nguyên
        spark.stop();
    }
}