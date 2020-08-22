package models;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DayAmountWrapper implements WritableComparable<DayAmountWrapper> {
    private Integer day;
    private Float amount;

    public DayAmountWrapper(Integer day, Float amount){
        set(day,amount);
    }

    public DayAmountWrapper(){
        set(new Integer(0),new Float(0));
    }

    public void set(Integer day,Float amount){
        this.day = day;
        this.amount = amount;
    }

    public Integer getDay() {
        return day;
    }

    public Float getAmount() {
        return amount;
    }

    @Override
    public int compareTo(DayAmountWrapper tripDayMonthWrapper) {
        int cmp = amount.intValue() - tripDayMonthWrapper.getAmount().intValue();
        if (cmp != 0) {
            return cmp;
        }
        return day.intValue() - tripDayMonthWrapper.getDay().intValue();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(amount);
        dataOutput.writeInt(day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        amount = dataInput.readFloat();
        day = dataInput.readInt();
    }
}
