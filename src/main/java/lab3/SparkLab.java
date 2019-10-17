package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkLab {

    private static final int ORIGIN_AIRPORT_ID_COLUMN = 11;
    private static final int DEST_AIRPORT_ID_COLUMN = 14;
    private static final int ARR_DELAY_TIME_COLUMN = 18;
    private static final int CANCELLED_COLUMN = 19;
    private static final String DEST_ID_HEAD_VALUE = "DEST_AIRPORT_ID";
    private static final String FLIGHTS_DATA_FILE_NAME = "664600583_T_ONTIME_sample.csv";

    private static Integer getOriginAirportId(String[] columns) {
        return Integer.parseInt(columns[ORIGIN_AIRPORT_ID_COLUMN]);
    }

    private static Integer getDestAirportId(String[] columns) {
        return Integer.parseInt(columns[DEST_AIRPORT_ID_COLUMN]);
    }

    private static float getDelayTime(String[] columns) {
        return columns[ARR_DELAY_TIME_COLUMN].equals("") ? 0f :
                Float.parseFloat(columns[ARR_DELAY_TIME_COLUMN]);
    }

    private static int isFlightDelayed(String[] columns) {
        return (columns[ARR_DELAY_TIME_COLUMN].equals("") ||
                Float.parseFloat(columns[ARR_DELAY_TIME_COLUMN]) == 0f) ?
                0 : 1;
    }

    private static int isFlightCancelled(String[] columns) {
        return Float.parseFloat(columns[CANCELLED_COLUMN]) == 0f ? 0 : 1;
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsFile = sc.textFile(FLIGHTS_DATA_FILE_NAME);

        JavaRDD<String[]> flightsColumns = flightsFile.map(s -> s.replaceAll("\"", "").split(","));
        JavaRDD<String[]> usefulFlightsColumns = flightsColumns.filter(
                arr -> !arr[DEST_AIRPORT_ID_COLUMN].equals(DEST_ID_HEAD_VALUE)
        );
        JavaPairRDD<Tuple2<Integer, Integer>, FlightStatistics> flightsStatisticsPairs = usefulFlightsColumns.mapToPair(
                arr -> new Tuple2<>(
                        new Tuple2<>(getOriginAirportId(arr), getDestAirportId(arr)),
                        new FlightStatistics(getDelayTime(arr), isFlightDelayed(arr), isFlightCancelled(arr), 1)
                )
        );
    }
}
