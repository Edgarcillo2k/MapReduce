package reducers;

import models.RegressionVariablesWrapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RegressionReducer extends Reducer<Text, RegressionVariablesWrapper, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<RegressionVariablesWrapper> values, Context context) throws IOException, InterruptedException {
        Double end = 0.0;
        Double init = 0.0;

        double sumY = 0;
        double sumX = 0;
        double sumXY = 0;
        double sumX2Y = 0;
        double sumX2 = 0;
        double sumX3 = 0;
        double sumX4 = 0;
        double n = 0;

        for(RegressionVariablesWrapper value : values) {
            init = value.getY();
            if(n > 0){
                double y = end / init;
                sumX += value.getX();
                sumY += y;
                sumX2 += Math.pow(value.getX(),2);
                sumX3 += Math.pow(value.getX(),3);
                sumX4 += Math.pow(value.getX(),4);
                sumXY += value.getX() * y;
                sumX2Y += Math.pow(value.getX(),2) * y;
            }
            end = value.getX();
            n++;
        }

        n--;



        /*Double b = ( (sumXY - ((sumX*sumY)/n))*(sumX4 - ((sumX2*sumX2)/n)) - (sumX2Y - ((sumX2*sumY)/n)) * (sumX3 - ((sumX2*sumX)/n)) ) /
                ( (sumX2 - ((sumX*sumX)/n)) * (sumX4 - ((sumX2*sumX2)/n)) - Math.pow((sumX3 - ((sumX2*sumX)/n)),2));
        Double c = (  (sumX2 - ((sumX*sumX)/n)) * (sumX2Y - ((sumX2*sumY)/n)) - (sumX3 - ((sumX2*sumX)/n)) * (sumXY - ((sumX*sumY)/n)) )  /
                ( (sumX2 - ((sumX*sumX)/n)) * (sumX4 - ((sumX2*sumX2)/n)) - Math.pow((sumX3 - ((sumX2*sumX)/n)),2) );

        Double a = (sumY - b * sumX - c * sumX2) / n;*/

        Double b = (n * sumXY - (sumY*sumX))/(n*sumX2-(sumX*sumX));

        Double a = (sumY - (b * sumX))/n;

        String bString = b.toString();//y = a + bx + cx^2
        //String cString = c.toString();
        String aString = a.toString();

        context.write(key, new Text( aString.concat("\t").concat(bString)));
        //context.write(key, new Text( aString.concat("\t").concat(bString).concat("\t").concat(cString)));
    }
}
