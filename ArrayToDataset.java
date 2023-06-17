import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Array to Dataset<String>")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[] {"Banana", "Car", "Banana", "Computer", "Car"};

        List<String> data = Arrays.asList(stringList);

        // dataframe
//        Dataset<Row> df = spark.createDataset(data, Encoders.STRING());
        // dataset
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());



        ds.printSchema();
        ds.show();


        Dataset<Row> df = ds.groupBy("value").count();
        df.show();

        Dataset<Row> df2 = ds.toDF();
        df2.as(Encoders.STRING()); // return a dataset
    }
}
