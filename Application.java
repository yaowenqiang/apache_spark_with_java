import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

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

        //transformation

//        df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));
        df = df.withColumn("full_name",
            concat(df.col("last_name"), lit(", "), df.col("first_name")))
            .filter(df.col("comment").rlike("\\d+"))
            .orderBy(df.col("last_name").asc());

        String dbConnectionUrl = "jdbc:postgresql://localhost/course_data";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user","postgres");
        prop.setProperty("password", "password");

        df.write()
            .mode(SaveMode.Overwrite)
            .jdbc(dbConnectionUrl, "project1", prop);

        df.show();

    }
}
