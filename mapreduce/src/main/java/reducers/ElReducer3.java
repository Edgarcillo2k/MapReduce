package reducers;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ElReducer3 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        float sumValues = 0;
        float promedio = 0;
        float init, end = 0, n = 0;

        for(FloatWritable value : values) {
            init = value.get();
            if(n>0){
                float crecimiento = end / init;
                System.out.print(crecimiento);
                sumValues += crecimiento;
            }
            end = value.get();
            n++;
        }
        n--;
        System.out.print(sumValues);
        System.out.print(n);
        promedio = sumValues / n;
        context.write(key, new FloatWritable(promedio));
    }
}
