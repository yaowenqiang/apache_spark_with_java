import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
public class Reddit {
    public static void main(String[] args) {
        Logger.getLogger("org.apache")
                .setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("reddit")
                .master("local")
                .getOrCreate();

        String customerFile = "reddit.json";
        Dataset<Row> customerDf = spark.read().format("json")
                .option("inferSchema", "true")
                .option("header", true)
                .load(customerFile);

    }
}
