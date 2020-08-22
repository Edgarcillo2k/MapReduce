import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ElReducer	extends Reducer<Text, FloatWritable, Text, FloatWritable> {
//public class ElReducer	extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    //public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    	
    	float sumaValues = 0;
    	for(FloatWritable value : values) {
            sumaValues += value.get();
    	}
        context.write(key, new FloatWritable(sumaValues));

    	/*float pasajeros = 0;
    	for(Text value : values) {
    	    pasajeros += 1;
        }
        context.write(key, new FloatWritable(pasajeros));*/
    }
}
