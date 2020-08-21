import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ElReducer	extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
    	
    	float sumaValues = 0;
    	for(FloatWritable value : values) {
            sumaValues += value.get();
    	}
    	
        context.write(key, new FloatWritable(sumaValues));
    }
}
