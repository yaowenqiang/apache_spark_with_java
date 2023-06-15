import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;

public class DefineCSVSchema {
    public void printDefinedSchema() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV with a schema to Dataframe")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(
                        "id",
                        DataTypes.IntegerType,
                        false
                ),
                DataTypes.createStructField(
                        "product_id",
                        DataTypes.IntegerType,
                        false
                ),
                DataTypes.createStructField(
                        "item_name",
                        DataTypes.StringType,
                        false
                ),
                DataTypes.createStructField(
                        "published_on",
                        DataTypes.DateType,
                        false
                ),
                DataTypes.createStructField(
                        "url",
                        DataTypes.StringType,
                        false
                ),
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
//                .option("multiline", true)
                .option("sep", ";")
                .option("dateformat","M/d/y")
                .schema(schema)
                .load("amazonProducts.txt");
        df.show(5,25);
        df.printSchema();

    }
}
