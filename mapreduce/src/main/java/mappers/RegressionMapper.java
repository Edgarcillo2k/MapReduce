package mappers;

import java.io.IOException;

import models.RegressionVariablesWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RegressionMapper extends Mapper<LongWritable, Text, Text, RegressionVariablesWrapper> {

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        String fields[] = lineText.toString().split("\t"); //origen + destino + anio + mes
        Text key = new Text(fields[0].concat("\t").concat(fields[1]).concat("\t").concat(fields[3]));
        RegressionVariablesWrapper values = new RegressionVariablesWrapper(Double.parseDouble(fields[2]),Double.parseDouble(fields[4]));
        context.write(key, values);
    }

}