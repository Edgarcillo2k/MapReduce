package mappers;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ElMaper3 extends Mapper<LongWritable, Text, Text, FloatWritable> {

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

        String fields[] = lineText.toString().split("\t"); //Input: Origen + Destino + Anio + Mes + Dia

        String paths = fields[0].concat("\t").concat(fields[1].concat("\t").concat(fields[2]).concat("\t").concat(fields[3])); //Result:  Origen + Destino + Anio + Mes

        Text key = new Text(paths);

        FloatWritable values = new FloatWritable(Float.parseFloat(fields[5])); // monto

        context.write(key, values);
    }


}