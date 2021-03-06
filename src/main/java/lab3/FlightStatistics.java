package lab3;

import java.io.Serializable;

class FlightStatistics implements Serializable {

    private float maxDelayTime;
    private long lateFlightsCount;
    private long cancelledFlightsCount;
    private long flightsCount;

    private float getMaxDelayTime() {
        return maxDelayTime;
    }

    private long getLateFlightsCount() {
        return lateFlightsCount;
    }

    private long getCancelledFlightsCount() {
        return cancelledFlightsCount;
    }

    private long getFlightsCount() {
        return flightsCount;
    }

    FlightStatistics(float maxDelayTime, long lateFlightsCount, long cancelledFlightsCount, long flightsCount) {
        this.maxDelayTime = maxDelayTime;
        this.lateFlightsCount = lateFlightsCount;
        this.cancelledFlightsCount = cancelledFlightsCount;
        this.flightsCount = flightsCount;
    }

    static FlightStatistics union(FlightStatistics statistics1, FlightStatistics statistics2) {
        return new FlightStatistics(
                Math.max(statistics1.getMaxDelayTime(), statistics2.getMaxDelayTime()),
                statistics1.getLateFlightsCount() + statistics2.getLateFlightsCount(),
                statistics1.getCancelledFlightsCount() + statistics2.getCancelledFlightsCount(),
                statistics1.getFlightsCount() + statistics2.getFlightsCount()
        );
    }

    @Override
    public String toString() {
        return maxDelayTime + "  " + ((float) lateFlightsCount / flightsCount * 100f) + "  " +
                ((float) cancelledFlightsCount / flightsCount * 100f);
    }
}
