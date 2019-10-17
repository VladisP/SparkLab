package lab3;

public class FlightStatistics {

    private int maxDelayTime;
    private long lateFlightsCount;
    private long cancelledFlightsCount;
    private long flightsCount;

    FlightStatistics(int maxDelayTime, long lateFlightsCount, long cancelledFlightsCount, long flightsCount) {
        this.maxDelayTime = maxDelayTime;
        this.lateFlightsCount = lateFlightsCount;
        this.cancelledFlightsCount = cancelledFlightsCount;
        this.flightsCount = flightsCount;
    }
}
