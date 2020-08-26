import mappers.RegressionMapper;
import models.RegressionVariablesWrapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import reducers.RegressionReducer;

public class StartJobMain extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		args = new String[6];
		args[0] = "Suma de montos por dias del mes por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosDiaMesRuta";
		args[3] = "mappers.ElMaper";
		args[4] = "reducers.ElReducer";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		int res = ToolRunner.run(new StartJobMain(), args);
		args = new String[6];
		args[0] = "Suma de pasajeros por dias del mes por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosDiaMesRuta";
		args[3] = "mappers.ElMaper";
		args[4] = "reducers.ElReducer2";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);
		args = new String[6];
		args[0] = "Suma de montos por mes de los anios por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosMesAnioRuta";
		args[3] = "mappers.ElMaper2";
		args[4] = "reducers.ElReducer";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);
		args = new String[6];
		args[0] = "Suma de pasajeros por mes de los anios por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosMesAnioRuta";
		args[3] = "mappers.ElMaper2";
		args[4] = "reducers.ElReducer2";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);
		args = new String[6];
		args[0] = "Regresion de montos por dias del mes por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosDiaMesRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/regresionMontosDiaMesRuta";
		args[3] = "mappers.RegressionMapper";
		args[4] = "reducers.RegressionReducer";
		args[5] = "models.RegressionVariablesWrapper";
		res = ToolRunner.run(new StartJobMain(), args);
		args = new String[6];
		args[0] = "Regresion de pasajeros por dias del mes por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosDiaMesRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/regresionPasajerosDiaMesRuta";
		args[3] = "mappers.RegressionMapper";
		args[4] = "reducers.RegressionReducer";
		args[5] = "models.RegressionVariablesWrapper";
		res = ToolRunner.run(new StartJobMain(), args);
		args = new String[6];
		args[0] = "Regresion de montos por mes de los anios por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosMesAnioRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/regresionMontosMesAnioRuta";
		args[3] = "mappers.RegressionMapper";
		args[4] = "reducers.RegressionReducer";
		args[5] = "models.RegressionVariablesWrapper";
		res = ToolRunner.run(new StartJobMain(), args);
		args = new String[6];
		args[0] = "Regresion de pasajeros por mes de los anios por ruta";
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosMesAnioRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/regresionPasajerosMesAnioRuta";
		args[3] = "mappers.RegressionMapper";
		args[4] = "reducers.RegressionReducer";
		args[5] = "models.RegressionVariablesWrapper";
		res = ToolRunner.run(new StartJobMain(), args);
		System.exit(0);
	}


	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), args[0]);
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass((Class<? extends Mapper>) Class.forName(args[3]));
		job.setReducerClass((Class<? extends Reducer>) Class.forName(args[4]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass((Class<? extends WritableComparable>)Class.forName(args[5]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}