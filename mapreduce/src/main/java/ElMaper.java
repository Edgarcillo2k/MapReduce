import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ElMaper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	
    	String fields[] = lineText.toString().split(","); // ["transactionid","fechaCompra","userId","monto","origen","destino"]

		String paths = fields[4].concat(fields[5]); // origen + destino
		FloatWritable monto = new FloatWritable(Float.parseFloat(fields[3]));
		Text ruta = new Text(paths);
		context.write(ruta, monto);

		//String dateparts = fields[1].split("/"); // año
		//IntWritable year = new IntWritable(Integer.parseInt(dateparts[2]));
		//FloatWritable monto = new FloatWritable(Float.parseFloat(fields[3]));
		//context.write(year, monto);

		//String dateparts = fields[1].split("/");
		//String dateofMonth = dateparts[0].concat(dateparts[1]); // dia + mes
		//Text key = new Text(dateofMonth);
		//FloatWritable value = new FloatWritable(Float.parseFloat(fields[3])); //monto
		//context.write(key, value);

		//System.out.println(paths);
    }
}