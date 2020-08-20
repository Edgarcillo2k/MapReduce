import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ElMaper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {
    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	
    	String fields[] = lineText.toString().split(","); // ["10/09/2010","4394396.21"]
    	String dateparts[] = fields[0].split("/"); // ["10","09","2010"]
    	
    	FloatWritable monto = new FloatWritable(Float.parseFloat(fields[1]));
    	IntWritable year = new IntWritable( Integer.parseInt(dateparts[2]));
    	
    	context.write(year, monto);
    	
    }
}