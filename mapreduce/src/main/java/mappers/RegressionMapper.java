package mappers;

import java.io.IOException;

import models.RegressionVariablesWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RegressionMapper extends Mapper<LongWritable, Text, Text, RegressionVariablesWrapper> {

    private String[] keyConcatIndexes;
    private Integer xVariableIndex;
    private Integer yVariableIndex;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        keyConcatIndexes = conf.getStrings("regression.key.concat.indexes",null);
        xVariableIndex = conf.getInt("regression.x.variable.index",0);
        yVariableIndex = conf.getInt("regression.x.variable.index",0);
        super.setup(context);
    }

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        String fields[] = lineText.toString().split(","); // ["mes","dia","origen","destino","monto"]
        StringBuilder concatKey = new StringBuilder();
        for(String index:keyConcatIndexes){
            concatKey = concatKey.append(fields[Integer.parseInt(index)]).append(",");
        }
        concatKey.deleteCharAt(concatKey.length()-1);
        Text key = new Text(concatKey.toString());
        RegressionVariablesWrapper values = new RegressionVariablesWrapper(Double.parseDouble(fields[xVariableIndex]),Double.parseDouble(fields[yVariableIndex]));
        context.write(key, values);
    }
}