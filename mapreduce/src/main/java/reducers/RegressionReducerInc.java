package reducers;

import models.RegressionVariablesWrapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RegressionReducerInc extends Reducer<Text, RegressionVariablesWrapper, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<RegressionVariablesWrapper> values, Context context) throws IOException, InterruptedException {

        double sumY = 0;
        double sumX = 0;
        double sumXY = 0;
        double sumX2 = 0;
        double n = 0;
        double init = 0.0;
        double end = 0;
        for(RegressionVariablesWrapper value : values) {
            init = value.getY();
            if(n>0){
                double crecimiento = end / init;
                sumX += value.getX();
                sumY += crecimiento;
                sumX2 += Math.pow(value.getX(),2);
                sumXY += value.getX() * crecimiento;
            }
            end = value.getY();
            n++;
        }
        n--;
        Double b = ((n* sumXY)-(sumX*sumY))/((n*sumX2)-(sumX*sumX));
        Double a = (sumY-(b*sumX))/n;
        String bString = b.toString();
        String aString = a.toString();
        context.write(key, new Text(aString.concat("\t").concat(bString)));

    }
}