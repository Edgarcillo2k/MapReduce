import java.io.IOException;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ElReducer	extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    public void reduce(Text rutas, Iterable<FloatWritable> montos, Context context)
        throws IOException, InterruptedException {
    	
    	float sumaMontos = 0;
    	for(FloatWritable monto : montos) {
    		sumaMontos += monto.get();
    	}
    	
        context.write(rutas, new FloatWritable(sumaMontos));
    }
}
