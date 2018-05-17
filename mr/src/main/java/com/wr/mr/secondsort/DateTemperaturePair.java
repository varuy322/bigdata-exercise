package com.wr.mr.secondsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by spark on 5/18/18.
 */
public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

    private String yearMonth;
    private String day;
    protected Integer temperature;


    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, yearMonth);
        out.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.yearMonth = Text.readString(in);
        this.temperature = in.readInt();
    }

    @Override
    public String toString() {
        return yearMonth.toString();
    }

    @Override
    public int compareTo(DateTemperaturePair o) {
        int compareValue = this.yearMonth.compareTo(o.getYearMonth());
        if (0 == compareValue) {
            compareValue = temperature.compareTo(o.getTemperature());
        }
        return -1 * compareValue;//desc sort
    }

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }

}
