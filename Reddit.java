import com.jobreadyprogrammer.spark.WordUtils;
import com.twitter.chill.java.ArraysAsListSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
public class Reddit {
    public static void main(String[] args) {
        Logger.getLogger("org.apache")
                .setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("reddit")
                .master("local")
                .getOrCreate();

        String customerFile = "RC_2007-01";
        Dataset<Row> redditDf = spark.read().format("json")
                .option("inferSchema", "true")
                .option("header", true)
                .load(customerFile);

//        redditDf.printSchema();
//        redditDf.show(10);
        redditDf = redditDf.select("body");
        redditDf.show(10);
        Dataset<String> wordsDs = redditDf.flatMap((FlatMapFunction<Row, String>)
                r -> Arrays.asList(r.toString()
                        .replace("\n", "")
                        .replace("\r", "")
                        .trim()
                        .toLowerCase()
                        .split(" "))
                        .iterator()
                , Encoders.STRING()
                );

        Dataset<Row> wordsDf = wordsDs.toDF();
        Dataset<Row> boringWordsDf = spark.createDataset(Arrays.asList(WordUtils.stopWords), Encoders.STRING()).toDF();

//        wordsDf = wordsDf.except(boringWordsDf); -- this remove duplicates

        wordsDf = wordsDf.join(boringWordsDf, wordsDf.col("value").equalTo(boringWordsDf.col("value")), "left_anti");

        wordsDf = wordsDf.groupBy("value").count();
        wordsDf.orderBy(desc("count")).show();



    }
}
