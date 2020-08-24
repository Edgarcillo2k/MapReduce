package reducers;

import models.RegressionVariablesWrapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ElReducer4 extends Reducer<Text, RegressionVariablesWrapper, Text, Text> {
    @Override

    public void reduce(Text key, Iterable<RegressionVariablesWrapper> values, Context context) throws IOException, InterruptedException {

        double sumY = 0;
        double sumX = 0;
        double sumXY = 0;
        double sumX2Y = 0;
        double sumX2 = 0;
        double sumX3 = 0;
        double sumX4 = 0;
        double n = 0;

        for(RegressionVariablesWrapper value : values) {

            float y,x;

            x = value.getX();
            y = value.getY();

            sumX += x;
            sumY += value.getY();
            sumX2 +=Math.pow(x,2);
            sumX3 += Math.pow(x,3);
            sumX4 += Math.pow(x,4);
            sumXY += x * y;
            sumX2Y += Math.pow(x,2) * y;


            n++;
        }

        Double b = ( (sumXY - ((sumX*sumY)/n))*(sumX4 - ((sumX2*sumX2)/n)) - (sumX2Y - ((sumX2*sumY)/n)) * (sumX3 - ((sumX2*sumX)/n)) ) /
                ( (sumX2 - ((sumX*sumX)/n)) * (sumX4 - ((sumX2*sumX2)/n)) - Math.pow((sumX3 - ((sumX2*sumX)/n)),2));
        Double c = (  (sumX2 - ((sumX*sumX)/n)) * (sumX2Y - ((sumX2*sumY)/n)) - (sumX3 - ((sumX2*sumX)/n)) * (sumXY - ((sumX*sumY)/n)) )  /
                ( (sumX2 - ((sumX*sumX)/n)) * (sumX4 - ((sumX2*sumX2)/n)) - Math.pow((sumX3 - ((sumX2*sumX)/n)),2) );

        Doable a = (sumY - b * sumX - c * sumX2) / n;

        String bString = b.toString();//y = b + 2cx
        String cString = c.toString();
        String aString = a.toString();

        context.write(key, new Text( aString.concat("\t").bString.concat("\t").concat(cString)));

    }
}
