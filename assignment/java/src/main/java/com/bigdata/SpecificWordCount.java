package com.bigdata;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SpecificWordCount {

    public static void main(String[] args) {
        // Khởi tạo SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("JavaSpecificWordCount")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // --- BƯỚC 1: Đọc danh sách các từ cần đếm và broadcast nó ---
        String targetWordsPath = "hdfs://namenode:9000/data/input/input.txt";
        
        // Đọc file, tách thành các từ, và thu thập danh sách về Driver
        List<String> targetWordsList = spark.read().textFile(targetWordsPath).javaRDD()
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .collect();
        
        // Tạo một broadcast variable từ danh sách.
        // Biến này sẽ được gửi hiệu quả đến tất cả các worker.
        Broadcast<Set<String>> broadcastTargetWords = sc.broadcast(new HashSet<>(targetWordsList));

        // --- BƯỚC 2: Đọc file văn bản chính ---
        String mainTextPath = "hdfs://namenode:9000/data/data.txt";
        JavaRDD<String> lines = spark.read().textFile(mainTextPath).javaRDD();

        // --- BƯỚC 3: Tách từ, lọc theo danh sách broadcast, và đếm ---
        JavaPairRDD<String, Integer> counts = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator()) // Tách dòng thành từ
                .filter(word -> broadcastTargetWords.value().contains(word)) // Lọc chỉ giữ lại các từ trong danh sách
                .mapToPair(word -> new Tuple2<>(word, 1)) // Chuyển mỗi từ thành cặp (từ, 1)
                .reduceByKey((a, b) -> a + b); // Cộng các giá trị cho cùng một từ

        // --- BƯỚC 4: In một phần kết quả và Ghi ra HDFS ---
        
        // In ra màn hình 20 kết quả đầu tiên để xem trước.
        List<Tuple2<String, Integer>> resultsSample = counts.take(20);
        System.out.println("--- XEM TRUOC 20 KET QUA DAU TIEN ---");
        for (Tuple2<String, Integer> tuple : resultsSample) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        System.out.println("---------------------------------------");

        // Ghi kết quả ra HDFS theo cách song song (chuẩn)
        String outputPath = "hdfs://namenode:9000/data/wordcount_output_java";
        counts.saveAsTextFile(outputPath);
        
        System.out.println("Hoan thanh dem tu! Kiem tra ket qua tai thu muc " + outputPath + " tren HDFS.");

        spark.stop();
    }
}