import mappers.TripDayMonthMapper;
import models.DayAmountWrapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import reducers.TripDayMonthReducer;

public class StartJobMain extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {	    
		int res = ToolRunner.run(new StartJobMain(), args);
	    System.exit(res);
	}


	public int run(String[] args) throws Exception {

	    Job job = new Job(getConf(), "Calculo de coeficientes de tarifas por dia");
	    getConf().set("mapred.textoutputformat.separator",",");
	    job.setJarByClass(this.getClass());

	    // System.out.println(args[0]);
	    // Use TextInputFormat, the default unless job.setInputFormatClass is used
	    FileInputFormat.addInputPath(job, new Path(args[0]));

	    //--Tablas Monto
	    FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/viajesdomesticoscr.db/tripDayMonthAmountCoefficients"));
		// FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/viajesdomesticoscr.db/dateValue"));

		//--Tablas Pasajero
		//FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/viajesdomesticoscr.db/dayMonthPass"));
		// FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/viajesdomesticoscr.db/datePass"));

	    job.setMapperClass(TripDayMonthMapper.class);
	    job.setReducerClass(TripDayMonthReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DayAmountWrapper.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}