import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {
    public static void main(String[] args) {
        // creat a session
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to db")
                .master("local")
                .getOrCreate();
        // get data

        Dataset<Row> df = spark.read().format("CSV")
                .option("header", true)
                .load("name_and_comments.txt");

        df.show();

    }
}
