package models;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RegressionVariablesWrapper implements WritableComparable<RegressionVariablesWrapper> {
    private Double x;
    private Double y;

    public RegressionVariablesWrapper(Double x,Double y){
        set(x,y);
    }

    public RegressionVariablesWrapper(){
        set(new Double(0),new Double(0));
    }

    public void set(Double x,Double y){
        this.x = x;
        this.y = y;
    }

    public Double getX() {
        return x;
    }

    public Double getY() {
        return y;
    }

    @Override
    public int compareTo(RegressionVariablesWrapper regressionVariablesWrapper) {
        int cmp = x.intValue() - regressionVariablesWrapper.getX().intValue();
        if (cmp != 0) {
            return cmp;
        }
        return y.intValue() - regressionVariablesWrapper.getY().intValue();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        x = dataInput.readDouble();
        y = dataInput.readDouble();
    }
}
