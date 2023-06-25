import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;

public class DatasetJoins {
    public static void main(String[] args) {
        Logger.getLogger("org.apache")
                .setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Learning spark sQL Dataframe API")
                .master("local")
                .getOrCreate();

        String studentsFile = "students.csv";
        Dataset<Row> studentDf = spark.read().format("csv")
                .option("inferSchema","true")
                .option("header", true)
                .load(studentsFile);
//        studentDf  = studentDf.withColumnRenamed("GPA", "gpa");
        studentDf.show();

        String gradeChartFile = "grade_chart.csv";
        Dataset<Row> gradeChartDf = spark.read().format("csv")
                .option("inferSchema","true")
                .option("header", true)
                .load(gradeChartFile);
        gradeChartDf.show();


//        studentDf = studentDf.join(gradeChartDf,studentDf.col("GPA")
//                 .equalTo(gradeChartDf.col("gpa")));
//        studentDf.join(gradeChartDf, "GPA").show();
//        studentDf.show();
//        Dataset<Row> joinedDf = studentDf.join(gradeChartDf,studentDf.col("GPA")
//                 .equalTo(gradeChartDf.col("gpa")));
        Dataset<Row> joinedDf = studentDf.join(gradeChartDf,studentDf.col("GPA").equalTo(gradeChartDf.col("gpa")));
//        Dataset<Row> joinedDf = studentDf.join(gradeChartDf,"gpa");
        joinedDf.printSchema();
        joinedDf.show(10);
//        joinedDf.filter(gradeChartDf.col("gpa").between(1, 3, 5));
//        joinedDf.select(studentDf.col("student_name")
//                , studentDf.col("favorite_book_title")
//                , gradeChartDf.col("letter_grade")
//        );
//        joinedDf.filter(col("gpa").between(1, 3.5));
//        joinedDf.select(col("student_name")
//                ,col("favorite_book_title")
//                ,col("letter_grade")
//        );
//        joinedDf.filter(col("gpa").between(1, 3.5))
//                .select("student_name" ,"favorite_book_title" ,"letter_grade" ).show();
//
        joinedDf.where(col("gpa").between(1, 3.5))
                .select("student_name" ,"favorite_book_title" ,"letter_grade" ).show();

        joinedDf.where(col("gpa").gt(1).and(gradeChartDf.col("gpt").lt(4.5)))
                .select("student_name" ,"favorite_book_title" ,"letter_grade" ).show();

        joinedDf.where(
                col("gpa").gt(1).and(gradeChartDf.col("gpa").lt(4.5))
                .or(gradeChartDf.col("gpa").equalTo(2.5))
                )
                .select("student_name" ,"favorite_book_title" ,"letter_grade" ).show();

    }
}
