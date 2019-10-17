package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import scala.Tuple2;

import java.util.Arrays;

public class SparkLab {

    private static final int DEST_AIRPORT_ID_COLUMN = 14;
    private static final String DEST_ID_HEAD_VALUE = "DEST_AIRPORT_ID";
    private static final String FLIGHTS_DATA_FILE_NAME = "664600583_T_ONTIME_sample.csv";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsFile = sc.textFile(FLIGHTS_DATA_FILE_NAME);

        JavaRDD<String[]> flightsColumns = flightsFile.map(s -> s.replaceAll("\"", "").split(","));
        JavaRDD<String[]> usefulFlightsColumns = flightsColumns.filter(
                arr -> !arr[DEST_AIRPORT_ID_COLUMN].equals(DEST_ID_HEAD_VALUE)
        );
        JavaPairRDD<Tuple2<Integer, Integer>, FlightStatistics>
    }
}
