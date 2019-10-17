package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import scala.Tuple2;

import java.util.Arrays;

public class SparkLab {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsFile = sc.textFile("664600583_T_ONTIME_sample.csv");

        JavaRDD<String[]> flightsColumns = flightsFile.map(s -> s.replaceAll("\"", "").split(","));
        JavaRDD<String[]> usefulFlightsColumns = flightsColumns.filter();

        //        JavaRDD<String> splitted = flightsFile.flatMap(
//                s -> Arrays.stream(s.replaceAll("\"", "")
//                        .split(","))
//                        .iterator()
//        );
//        JavaPairRDD<Tuple2<Integer, Integer>, >
    }
}
