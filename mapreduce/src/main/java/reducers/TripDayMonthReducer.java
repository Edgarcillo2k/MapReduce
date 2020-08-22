package reducers;

import java.io.IOException;

import models.DayAmountWrapper;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class TripDayMonthReducer extends Reducer<Text, DayAmountWrapper, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<DayAmountWrapper> values, Context context)
            throws IOException, InterruptedException {

        double sumY = 0;
        double sumX = 0;
        double sumXY = 0;
        double sumX2Y = 0;
        double sumX2 = 0;
        double sumX3 = 0;
        double sumX4 = 0;
        double n = 0;

        for(DayAmountWrapper value : values) {
            sumY += value.getAmount();
            sumX += value.getDay();
            sumX2 += Math.pow(value.getDay(),2);
            sumX3 += Math.pow(value.getDay(),3);
            sumX4 += Math.pow(value.getDay(),4);
            sumXY += value.getAmount()*value.getDay();
            sumX2Y += Math.pow(value.getDay(),2)*value.getDay();
            n++;
        }
        Double b = ( (sumXY - ((sumX*sumY)/n))*(sumX4 - ((sumX2*sumX2)/n)) - (sumX2Y - ((sumX2*sumY)/n)) * (sumX3 - ((sumX2*sumX)/n)) ) /
                ( (sumX2 - ((sumX*sumX)/n)) * (sumX4 - ((sumX2*sumX2)/n)) - Math.pow((sumX3 - ((sumX2*sumX)/n)),2));
        Double c = (  (sumX2 - ((sumX*sumX)/n)) * (sumX2Y - ((sumX2*sumY)/n)) - (sumX3 - ((sumX2*sumX)/n)) * (sumXY - ((sumX*sumY)/n)) )  /
                ( (sumX2 - ((sumX*sumX)/n)) * (sumX4 - ((sumX2*sumX2)/n)) - Math.pow((sumX3 - ((sumX2*sumX)/n)),2) );
        String bString = b.toString();//y = b + 2c
        String cString = c.toString();
        context.write(key, new Text(bString.concat(",").concat(cString)));
    }
}
