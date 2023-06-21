import org.apache.log4j.Level;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
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
        df.show();
        df.printSchema();

        Dataset<House> ds = df.map(new HouseMapper(), Encoders.bean(House.class));
        ds.show();
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
