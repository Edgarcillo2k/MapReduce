package mappers;

import models.RegressionVariablesWrapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ElMaper4 extends Mapper<LongWritable, Text, Text, RegressionVariablesWrapper> {

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

        String fields[] = lineText.toString().split("\t"); //Input: Origen + Destino + Anio + Mes

        String paths = fields[0].concat("\t").concat(fields[1].concat("\t").concat(fields[3])); //Result:  origen + destino + mes

        Text key = new Text(paths);

        RegressionVariablesWrapper values = new RegressionVariablesWrapper(Double.parseDouble(fields[2]),Double.parseDouble(fields[4]));

        context.write(key, values);
    }


}