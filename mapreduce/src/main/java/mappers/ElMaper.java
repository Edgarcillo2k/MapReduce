package mappers;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ElMaper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	
    	String fields[] = lineText.toString().split("\001"); // ["transactionid","fechaCompra","userId","monto","origen","destino"]

		String paths = fields[4].concat("/").concat(fields[5]); // origen + destino
		String date = fields[1].split(" ")[0];
		String[] dateparts = date.split("-"); // fecha

		//-----Tablas Monto
		String datePath = dateparts[1].concat("/").concat(dateparts[2]).concat("/").concat(paths); // mes + dia + ruta
		//String datePath = fields[1].concat(paths); // fecha + ruta

		Text key = new Text(datePath);
		FloatWritable values = new FloatWritable(Float.parseFloat(fields[3])); // monto

		//-------------------------------------------------------------------
		//-----Tablas Pasajero
		//Text values = new Text(fields[1])); // pasajero

		context.write(key, values);
    }
}