import org.apache.log4j.Level;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) {
        Logger.getLogger("org.apache")
                .setLevel(Level.WARN);
        String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" +
                "'for', 'if', 'in', 'into', 'is', 'it',\r\n" +
                "'no', 'not', 'of', 'on', 'or', 'such',\r\n" +
                "'that', 'the', 'their', 'then', 'there', 'these',\r\n" +
                "'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," +
                "'your', 'you', 'I', "
                + " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";

        SparkSession spark = SparkSession.builder().appName("unstructured text to flatmap")
                .master("local")
                .getOrCreate();

        String filename = "shakespeare.txt";
        Dataset<Row> df = spark.read().format("text")
                .load(filename);

//        df.printSchema();
//        df.show();
        Dataset<String> wordDS = df.flatMap(new LineMapper(), Encoders.STRING());
        wordDS.show(100);
        Dataset<Row> df2 = wordDS.toDF();
        df2.printSchema();
        df2 = df2.groupBy("value").count();
        df2 = df2.orderBy(df2.col("count").desc());
//        df2 = df2.filter("lower(value) NOT IN " + boringWords);
        df2 = df2.filter("lower(value) IN ('love')");
        // spark will use the catalyst optimizer on the dataframe, which means it will do the filter first ,then
//        group by and order by
        df2.show(20);

    }

    public  static class LineMapper implements FlatMapFunction<Row, String> {
        @Override
        public Iterator call(Row row) throws Exception {
            return Arrays.asList(row.toString().toLowerCase().split(" ")).iterator();
        }
    }
}
