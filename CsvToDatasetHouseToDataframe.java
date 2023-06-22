import org.apache.log4j.Level;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;

public class CsvToDatasetHouseToDataframe {
    public static void main(String[] args) {
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
        System.out.println("House ingested in a dataframe:");
        df.show();
        df.printSchema();

        Dataset<House> ds = df.map(new HouseMapper(), Encoders.bean(House.class));
        System.out.println("House ingested in a dataset:");
        ds.show();
        ds.printSchema();

        Dataset<Row> df2 = ds.toDF();

        // TODO vacantBy.year value is wrong

        df2 = df2.withColumn("formattedDate", concat(df2.col("vacantBy.date"),lit("_"), df2.col("vacantBy.year")));
        df2.show();
        df2.printSchema();

    }

    static class HouseMapper implements MapFunction<Row, House> {
        @Override
        public House call(Row row) throws Exception {
            House h = new House();
            h.setId(row.getAs("id"));
            h.setAddress(row.getAs("address"));
            h.setSqft(row.getAs("sqft"));
            h.setPrice(row.getAs("price"));
            String vacancyDateString = row.getAs("vacantBy").toString();
            if (vacancyDateString != null) {
                SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd");
                h.setVacantBy(parser.parse(vacancyDateString));
            } else {
                h.setVacantBy(null);
            }
            return h;
        }
    }
}
