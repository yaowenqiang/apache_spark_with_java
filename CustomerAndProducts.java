import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomerAndProducts {
    public static void main(String[] args) {
        Logger.getLogger("org.apache")
                .setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Learning spark sQL Dataframe API")
                .master("local")
                .getOrCreate();

        String customerFile = "customers.csv";
        Dataset<Row> customerDf = spark.read().format("csv")
                .option("inferSchema","true")
                .option("header", true)
                .load(customerFile);
        customerDf.show();

        String productFile = "products.csv";
        Dataset<Row> productDf = spark.read().format("csv")
                .option("inferSchema","true")
                .option("header", true)
                .load(productFile);
        productDf.show();

        String purchasesFile = "purchases.csv";
        Dataset<Row> purchasesDf = spark.read().format("csv")
                .option("inferSchema","true")
                .option("header", true)
                .load(purchasesFile);
        purchasesDf.show();

        Dataset<Row> joinedDf = customerDf.join(purchasesDf,customerDf.col("customer_id").equalTo(purchasesDf.col("customer_id")))
                                            .join(productDf,purchasesDf.col("product_id").equalTo(productDf.col("product_id")))
                                            .drop("favorite_website")
                                            .drop(purchasesDf.col("customer_id"))
                                            .drop(purchasesDf.col("product_id"))
                                            .drop(productDf.col("product_id"));
        joinedDf.printSchema();
        joinedDf.groupBy("first_name").count().show();
        joinedDf.groupBy("product_name", "product_name").agg(
                count("product_name").as("number_of_purchases"),
                max("product_price").as("most_exp_purchases")
                        ).show();
//        joinedDf.show(1);
    }
}
