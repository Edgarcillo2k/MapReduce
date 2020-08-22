package mappers;

import models.RegressionVariablesWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CollectionMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private String[] keyConcatIndexes;
    private Integer outIndex;
    private String separator;
    private Integer dateIndex;
    private String[] keyConcatDateIndexes;

    @Override
    protected void setup(Mapper.Context context) {
        Configuration conf = context.getConfiguration();
        System.out.println("conf1");
        keyConcatIndexes = conf.getStrings("key.concat.indexes",null);
        System.out.println("conf2");
        outIndex = conf.getInt("out.index",0);
        System.out.println("conf3");
        separator = conf.get("separator","\001");
        System.out.println("conf4");
        dateIndex = conf.getInt("date.index",0);
        System.out.println("conf5");
        keyConcatDateIndexes = conf.getStrings("key.concat.date.indexes",null);
        System.out.println("conf6");
    }

    public void map(LongWritable offset, Text lineText, Mapper.Context context) throws IOException, InterruptedException {
        System.out.println("fields1");
        String fields[] = lineText.toString().split(separator); //["transactionid","fechaCompra","userId","monto","origen","destino"]
        String dateFields[] = fields[dateIndex].split(" ")[0].split("-");
        //["anio-mes-dia","tiempo"]
        System.out.println("fields2");
        StringBuilder concatKey = new StringBuilder();
        for(String index:keyConcatIndexes){
            concatKey = concatKey.append(fields[Integer.parseInt(index)]).append(",");
        }
        System.out.println("fields3");
        for(String index:keyConcatDateIndexes){
            concatKey = concatKey.append(dateFields[Integer.parseInt(index)]).append(",");
        }
        System.out.println("fields4");
        concatKey.deleteCharAt(concatKey.length()-1);
        System.out.println("fields5");
        Text key = new Text(concatKey.toString());
        FloatWritable values = new FloatWritable(Float.parseFloat(fields[outIndex]));
        context.write(key, values);
    }
}
