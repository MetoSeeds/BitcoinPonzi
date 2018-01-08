package it.unica.tcs.bitcoin.parser.Model;

/**
 * Created by Sergio Serusi on 15/11/2017.
 */
public class Responsiveness {
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long avg;

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getAvg() {
        return avg;
    }

    public void setAvg(long avg) {
        this.avg = avg;
    }
}
