import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class FileStreamingApplication {
    public static void main(String[] args) {

        // nc -lk 9999

//        Logger.getLogger("org.apache")
//                .setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("FileStreamingApplication")
                .master("local")
                .getOrCreate();

        StructType useSchema = new StructType().add("date", "string").add("value", "float");

        Dataset<Row> stockData = spark
                .readStream()
                .option("sep", ",")
                .schema(useSchema);
                .csv("csvpath");

    }
}
