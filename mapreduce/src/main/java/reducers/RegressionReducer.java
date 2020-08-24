package reducers;

import models.RegressionVariablesWrapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RegressionReducer extends Reducer<Text, RegressionVariablesWrapper, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<RegressionVariablesWrapper> values, Context context) throws IOException, InterruptedException {

        double sumY = 0;
        double sumX = 0;
        double sumXY = 0;
        double sumX2 = 0;
        double n = 0;
        for(RegressionVariablesWrapper value : values) {
            sumX += value.getX();
            sumY += value.getY();
            sumX2 += Math.pow(value.getX(),2);
            sumXY += value.getX() * value.getY();
            n++;
        }

        Double b = ((n* sumXY)-(sumX*sumY))/((n*sumX2)-(sumX*sumX));
        String bString = b.toString();

        context.write(key, new Text(bString));

    }
}