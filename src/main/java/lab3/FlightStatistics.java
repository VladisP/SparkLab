package lab3;

import java.io.Serializable;

public class FlightStatistics implements Serializable {

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
