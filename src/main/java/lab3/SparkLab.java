package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class SparkLab {

    private static final int ORIGIN_AIRPORT_ID_COLUMN = 11;
    private static final int DEST_AIRPORT_ID_COLUMN = 14;
    private static final int ARR_DELAY_TIME_COLUMN = 18;
    private static final int CANCELLED_COLUMN = 19;
    private static final int AIRPORT_ID_COLUMN = 0;
    private static final int AIRPORT_NAME_COLUMN = 1;
    private static final String DEST_ID_HEAD_VALUE = "DEST_AIRPORT_ID";
    private static final String ID_HEAD_VALUE = "Code";
    private static final String FLIGHTS_DATA_FILE_NAME = "664600583_T_ONTIME_sample.csv";
    private static final String AIRPORTS_DATA_FILE_NAME = "L_AIRPORT_ID.csv";
    private static final String OUTPUT_DATA_FILE_NAME = "outputLab3";

    private static Integer getId(String[] columns, int idColumnNumber) {
        return Integer.parseInt(columns[idColumnNumber]);
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

    private static String getAirportName(String[] columns) {
        return columns.length == 3 ?
                columns[AIRPORT_NAME_COLUMN] + columns[AIRPORT_NAME_COLUMN + 1] :
                columns[AIRPORT_NAME_COLUMN];
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsFile = sc.textFile(FLIGHTS_DATA_FILE_NAME);
        JavaRDD<String[]> flightsColumns = flightsFile.map(CsvParser::getColumns);
        JavaRDD<String[]> usefulFlightsColumns = flightsColumns.filter(
                arr -> !arr[DEST_AIRPORT_ID_COLUMN].equals(DEST_ID_HEAD_VALUE)
        );
        JavaPairRDD<Tuple2<Integer, Integer>, FlightStatistics> primaryStatisticsPairs = usefulFlightsColumns.mapToPair(
                arr -> new Tuple2<>(
                        new Tuple2<>(getId(arr, ORIGIN_AIRPORT_ID_COLUMN), getId(arr, DEST_AIRPORT_ID_COLUMN)),
                        new FlightStatistics(getDelayTime(arr), isFlightDelayed(arr), isFlightCancelled(arr), 1)
                )
        );
        JavaPairRDD<Tuple2<Integer, Integer>, FlightStatistics> statisticsPairs = primaryStatisticsPairs.reduceByKey(
                FlightStatistics::union
        );

        JavaRDD<String> airportsFile = sc.textFile(AIRPORTS_DATA_FILE_NAME);
        JavaRDD<String[]> airportsColumns = airportsFile.map(CsvParser::getColumns);
        JavaRDD<String[]> usefulAirportsColumns = airportsColumns.filter(
                arr -> !arr[AIRPORT_ID_COLUMN].equals(ID_HEAD_VALUE)
        );
        JavaPairRDD<Integer, String> airportDataPairs = usefulAirportsColumns.mapToPair(
                arr -> new Tuple2<>(getId(arr, AIRPORT_ID_COLUMN), getAirportName(arr))
        );
        Map<Integer, String> airportDataMap = airportDataPairs.collectAsMap();

        final Broadcast<Map<Integer, String>> airportsBroadcast = sc.broadcast(airportDataMap);

        JavaRDD<String> result = statisticsPairs.map(
                p -> airportsBroadcast.value().get(p._1._1) + "  |  " +
                        airportsBroadcast.value().get(p._1._2) + "  |  " +
                        p._2.toString()
        );
        result.saveAsTextFile(OUTPUT_DATA_FILE_NAME);
    }
}
