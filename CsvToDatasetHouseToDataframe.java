import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
public class CsvToDatasetHouseToDataframe {
    public void  start() {

        Logger.getLogger("org.apache")
                .setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("CSV to dataframe to Dataset<House> adn back")
                .master("local")
                .getOrCreate();

        String filename = "houses.csv";
        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema","true")
                .option("header", true)
                .option("sep",";")
                .load(filename);


    }
}
