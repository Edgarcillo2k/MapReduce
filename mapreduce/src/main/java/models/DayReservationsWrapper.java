package models;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DayReservationsWrapper implements WritableComparable<DayReservationsWrapper> {
    private Integer day;
    private Integer reservations;

    public DayReservationsWrapper(Integer day, Integer reservations){
        set(day,reservations);
    }

    public DayReservationsWrapper(){
        set(new Integer(0),new Integer(0));
    }

    public void set(Integer day,Integer reservations){
        this.day = day;
        this.reservations = reservations;
    }

    public Integer getDay() {
        return day;
    }

    public Integer getReservations() {
        return reservations;
    }

    @Override
    public int compareTo(DayReservationsWrapper dayReservationWrapper) {
        int cmp = reservations - dayReservationWrapper.getReservations();
        if (cmp != 0) {
            return cmp;
        }
        return day - dayReservationWrapper.getDay();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(reservations);
        dataOutput.writeInt(day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reservations = dataInput.readInt();
        day = dataInput.readInt();
    }
}
