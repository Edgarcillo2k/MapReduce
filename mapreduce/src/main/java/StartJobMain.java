import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;


public class StartJobMain extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {	    
		int res = ToolRunner.run(new StartJobMain(), args);
	    System.exit(res);
	}

	public int run(String[] args) throws Exception {

	    Job job = new Job(getConf(), "Calculo de Tarifas");
	    job.setJarByClass(this.getClass());

	    // System.out.println(args[0]);
	    // Use TextInputFormat, the default unless job.setInputFormatClass is used
	    FileInputFormat.addInputPath(job, new Path(args[0]));
		ArrayList<String> rutas = new ArrayList<>();
	    FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/viajesdomesticoscr.db/rutamonto"));
		// FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/viajesdomesticoscr.db/"));
		// FileOutputFormat.setOutputPath(job, new Path("/user/hive/warehouse/viajesdomesticoscr.db/"));
	    job.setMapperClass(ElMaper.class);
	    job.setReducerClass(ElReducer.class);
	    job.setOutputKeyClass(Text.class); 				// ruta
	    job.setOutputValueClass(FloatWritable.class); 	// monto
		//job.setOutputKeyClass(Text.class); 				// ruta
		//job.setOutputValueClass(FloatWritable.class); 	// monto
		//job.setOutputKeyClass(Text.class); 				// ruta
		//job.setOutputValueClass(FloatWritable.class); 	// monto
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}