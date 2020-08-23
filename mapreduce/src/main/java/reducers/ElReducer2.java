package reducers;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ElReducer2 extends Reducer<Text, FloatWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
    	int sumaValues = 0;
    	for(FloatWritable value : values) {
            sumaValues ++;
    	}
        context.write(key, new IntWritable(sumaValues));
    }
}
