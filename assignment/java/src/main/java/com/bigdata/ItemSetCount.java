package com.bigdata;

// Thêm các import cần thiết cho việc tương tác với HDFS
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Chương trình Spark để tính độ support của từng item,
 * sắp xếp kết quả tăng dần, định dạng, tự động xóa output cũ
 * và lưu vào một file duy nhất.
 */
public class ItemSetCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws IOException { // Thêm throws IOException
        SparkSession spark = SparkSession.builder()
                .appName("JavaItemSupportCountSorted")
                .getOrCreate();

        String inputPath = "hdfs://namenode:9000/data/kosarak10k.txt";
        JavaRDD<String> lines = spark.read().textFile(inputPath).javaRDD();

        // --- BƯỚC 1: TÍNH TOÁN SUPPORT COUNT ---
        JavaPairRDD<String, Integer> itemSupports = lines
                .flatMap(line -> Arrays.asList(SPACE.split(line)).iterator())
                .filter(item -> !item.isEmpty())
                .mapToPair(item -> new Tuple2<>(item, 1))
                .reduceByKey((a, b) -> a + b);

        // --- BƯỚC 2: CHUYỂN KEY SANG INTEGER VÀ SẮP XẾP ---
        JavaPairRDD<Integer, Integer> itemSupportsIntKeys = itemSupports
                .mapToPair(tuple -> new Tuple2<>(Integer.parseInt(tuple._1()), tuple._2()));

        JavaPairRDD<Integer, Integer> sortedSupports = itemSupportsIntKeys.sortByKey();

        // --- BƯỚC 3: ĐỊNH DẠNG LẠI RDD ĐÃ SẮP XẾP ---
        JavaRDD<String> formattedResults = sortedSupports.map(tuple -> tuple._1() + ": " + tuple._2());

    
        // --- BƯỚC 5: LƯU KẾT QUẢ VÀO FILE DUY NHẤT ---
        String outputPath = "hdfs://namenode:9000/data/item_support_output_temp";

        // Gộp RDD về 1 partition và lưu kết quả
        JavaRDD<String> singlePartitionRdd = formattedResults.coalesce(1);
        singlePartitionRdd.saveAsTextFile(outputPath);

        System.out.println("Hoàn thành! Đã lưu kết quả vào thư mục: " + outputPath);
        System.out.println("Chạy lệnh HDFS sau để đổi tên file kết quả thành result.txt:");
        System.out.println("hdfs dfs -mv " + outputPath + "/part-00000 /data/result.txt");

        spark.stop();
    }
}