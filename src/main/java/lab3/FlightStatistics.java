package lab3;

import java.io.Serializable;

public class FlightStatistics implements Serializable {

    private float maxDelayTime;
    private long lateFlightsCount;
    private long cancelledFlightsCount;
    private long flightsCount;

    FlightStatistics(float maxDelayTime, long lateFlightsCount, long cancelledFlightsCount, long flightsCount) {
        this.maxDelayTime = maxDelayTime;
        this.lateFlightsCount = lateFlightsCount;
        this.cancelledFlightsCount = cancelledFlightsCount;
        this.flightsCount = flightsCount;
    }

    static FlightStatistics union(FlightStatistics statistics1, FlightStatistics statistics2) {
        return new FlightStatistics(Math.max(statistics1.maxDelayTime, statistics2.maxDelayTime));
    }
}
