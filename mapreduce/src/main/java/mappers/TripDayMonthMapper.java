package mappers;

import models.DayAmountWrapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TripDayMonthMapper extends Mapper<LongWritable, Text, Text, DayAmountWrapper> {
    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException {

        String[] fields = lineText.toString().split(","); // ["mes/dia/origen/destino","monto"]
        String[] keyFields = fields[0].split("/");
        String trip = keyFields[2].concat("/").concat(keyFields[3]); //origen+destino
        String month = keyFields[0];
        Integer day = Integer.parseInt(keyFields[1]);
        String monthTrip = month.concat("/").concat(trip); //mes + ruta
        Text key = new Text(monthTrip);
        DayAmountWrapper tripDayMonthWrapper = new DayAmountWrapper(day,Float.parseFloat(fields[1])); //dia y monto
        context.write(key, tripDayMonthWrapper);
    }
}
