package mappers;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ElMaper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
    	String fields[] = lineText.toString().split("\001"); // ["transactionid","fechaCompra","userId","monto","origen","destino"]
		String paths = fields[4].concat("\t").concat(fields[5]); // origen + destino
		String date = fields[1].split(" ")[0];
		String[] dateparts = date.split("-"); // fecha
		String datePath = paths.concat("\t").concat(dateparts[2]).concat("\t").concat(dateparts[1]); //origen + destino + dia + mes
		Text key = new Text(datePath);
		FloatWritable values = new FloatWritable(Float.parseFloat(fields[3])); // monto
		context.write(key, values);
    }
}