import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class JSONLineParser {
    public void parseJsonLines() {

        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().format("json")
                .load("simple.json");

        Dataset<Row> df2 = spark.read().format("json")
                .option("multiline", true)
                .load("multiline.json");
        df.show();
        df.printSchema();

        df2.show();
        df2.printSchema();
    }
}
