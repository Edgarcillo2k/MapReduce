import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
// import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ElReducer	extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
    @Override
    public void reduce(IntWritable year, Iterable<FloatWritable> montos, Context context)
        throws IOException, InterruptedException {
    	
    	float sumaMontos = 0;
    	for(FloatWritable monto : montos) {
    		sumaMontos += monto.get();
    	}
    	
        context.write(year, new FloatWritable(sumaMontos));
    }
}
