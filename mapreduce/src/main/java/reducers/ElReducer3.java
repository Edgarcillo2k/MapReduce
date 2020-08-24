

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ElReducer3 extends Reducer<Text, RegressionVariablesWrapper, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        float sumValues = 0, promedio ;
        float init, end , n;

        for(FloatWritable value : values) {

            init = value.get();
            if(n>0){
                sumValues += end / init;
            }
            end = value.get();

            n++;

        }

        n--;
        promedio = sumValues / n;

        context.write(key, new FloatWritable(promedio));
    }
}
