import org.apache.log4j.Level;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
public class MapAndReduce {
    public static void main(String[] args) {

        Logger.getLogger("org.apache")
                .setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Map and Reduce")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[] {"banana", "Car", "Glass", "Banana", "Computer", "Car"};

        List<String> data = Arrays.asList(stringList);

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds = ds.map(new StringMapper(), Encoders.STRING());
        ds = ds.map((MapFunction<String, String>) row -> "hello " + row, Encoders.STRING());
        ds.show();
//        String str = ds.reduce((ReduceFunction<String>) row1, row2 -> row1 + row2 , Encoders.STRING());
        String str = ds.reduce(new StringReducer());
        System.out.println(str);
    }

    static class  StringMapper implements MapFunction<String, String>, Serializable {
        @Override
        public String call(String s) throws Exception {
            return "word: " + s;
        }
    }

    static class StringReducer implements ReduceFunction<String>, Serializable {
        @Override
        public String call(String o, String t1) throws Exception {
            return o + "," + t1 + "\n";
        }
    }


}
