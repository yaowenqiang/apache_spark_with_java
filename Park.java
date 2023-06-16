import org.apache.spark.Partition;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.*;

public class Park {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 datasets")
                .master("local")
                .getOrCreate();
        Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark);
        durhamDf.printSchema();
//        durhamDf.show(10);

        Dataset<Row> philDf = buildPhilParkDataFrame(spark);
        philDf.printSchema();
//        philDf.show(10);
////        philDf.count();
        combineDataFrames(durhamDf, philDf);
    }

    public static Dataset<Row> buildDurhamParksDataFrame(SparkSession spark) {
        Dataset<Row>  df =  spark.read()
                .format("json")
                .option("multiline", true)
                .load("durham-parks.json");
        df = df.withColumn("park_id",concat(df.col("datasetid"), lit("_"),
                df.col("fields.objectid"),lit("_durham")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", lit("Durham"))
                .withColumn("address", df.col("fields.address"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acres",df.col("fields.acres"))
                .withColumn("geox", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoy", df.col("geometry.coordinates").getItem(1))
                .drop("fields")
                .drop("geometry")
                .drop("record_timestamp")
                .drop("recordid")
                .drop("datasetid")

        ;
        return df;
    }

    public static Dataset<Row> buildPhilParkDataFrame(SparkSession spark) {
        Dataset<Row> df =  spark.read()
                .format("csv")
                .option("sep", ",")
                .option("header", "true")
                .load("philadelphia_recreations.csv");
//                df = df.filter(lower(df.col("USE_")).like("%Park%"));
        df = df.filter("lower(USE_) like '%park%' ");
        df = df.withColumn("park_id",concat(lit("phil_"), df.col("objectid")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumn("city", lit("philadelphia"))
                .withColumnRenamed("ADDRESS", "address")
                .withColumn("has_playground", lit("unknown"))
                .withColumnRenamed("ZIPCODE", ("zipcode"))
                .withColumnRenamed("ACREAGE","land_in_acres")
                .withColumn("geox", lit("unknown"))
                .withColumn("geoy", lit("unknown"))
                .drop("SITE_NAME")
                .drop("OBJECTID")
                .drop("CHILD_OF")
                .drop("TYPE")
                .drop("USE_")
                .drop("DESCRIPTION")
                .drop("SQ_FEET")
                .drop("ALIAS")
                .drop("CHRONOLOGY")
                .drop("NOTES")
                .drop("DATE_EDITED")
                .drop("EDITED_BY")
                .drop("OCCUPANT")
                .drop("TENANT")
                .drop("LABEL")
                .drop("ALLIAS")
        ;

        return df;
    }

    public static void combineDataFrames(Dataset<Row> df1, Dataset<Row> df2) {
        // Match by column names using the unionByName method
        // if we use just the union() method, it matches the columns based on order.
        Dataset<Row> df = df1.unionByName(df2);
        df.printSchema();
//        System.out.println(df.count());
        df.show(10);
        df = df.repartition(5);
        Partition[] partitions = df.rdd().partitions();
        System.out.println("Total number of Partitions: " + partitions.length);
    }
}
