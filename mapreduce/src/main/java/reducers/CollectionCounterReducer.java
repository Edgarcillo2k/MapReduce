package reducers;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CollectionCounterReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float counter = 0;
        for(FloatWritable value : values) {
            counter++;
        }
        context.write(key, new FloatWritable(counter));
    }
}
