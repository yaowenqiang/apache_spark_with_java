import com.jobreadyprogrammer.spark.WordUtils;
import com.twitter.chill.java.ArraysAsListSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


import javax.xml.crypto.Data;
import java.util.Arrays;

public class StreamingSocketApplication {
    public static void main(String[] args) {

        // nc -lk 9999

//        Logger.getLogger("org.apache")
//                .setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("StreamingSocketApplication")
                .master("local")
                .getOrCreate();

        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> wordCounts = words.groupBy("value").count();
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException exception) {
            System.out.println(exception.cause());
        }



    }
}
