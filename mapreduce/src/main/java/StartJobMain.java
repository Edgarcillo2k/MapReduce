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
		args[0] = "Suma de montos por año y mes por ruta"; 				//LISTO  (1)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosAnioMesRuta";
		args[3] = "mappers.ElMaper";
		args[4] = "reducers.ElReducer";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);

		args[0] = "Suma de pasajeros por año y mes por ruta"; 				//LISTO  (2)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosAnioMesRuta";
		args[3] = "mappers.ElMaper";
		args[4] = "reducers.ElReducer2";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);


		args[0] = "Suma de pasajeros por anio, mes y dia por ruta"; 				//LISTO  (3)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosAnioMesDiaRuta";
		args[3] = "mappers.ElMaper2";
		args[4] = "reducers.ElReducer2";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);


		args[0] = "Suma de montos por anio, mes y dia por ruta"; 				//LISTO  (4)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosAnioMesDiaRuta";
		args[3] = "mappers.ElMaper2";
		args[4] = "reducers.ElReducer";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);

		//---------------------------- INCREMENTOS ---------------------------------------------
		args[0] = "Incremento pasajeros por dia por anio"; 							//LISTO (5)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosAnioMesDiaRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/incPasajerosAnioMesDiaRuta";
		args[3] = "mappers.ElMaper3";
		args[4] = "reducers.ElReducer3";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);


		args[0] = "Incremento montos por dia por anio"; 							//LISTO (6)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosAnioMesDiaRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/incMontosAnioMesDia";
		args[3] = "mappers.ElMaper3";
		args[4] = "reducers.ElReducer3";
		args[5] = "org.apache.hadoop.io.FloatWritable";
		res = ToolRunner.run(new StartJobMain(), args);


		//------------------------------REGRESIONES ------------------------------------------------
		args[0] = "Regresion pasajeros por mes por anio";									//LISTO (7)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosAnioMesRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/regresionPasajerosAnioMesRuta";
		args[3] = "mappers.RegressionMapper";
		args[4] = "reducers.RegressionReducer";
		args[5] = "models.RegressionVariablesWrapper";
		res = ToolRunner.run(new StartJobMain(), args);

		args[0] = "Regresion montos por mes por anio";									//LISTO (8)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosAnioMesRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/regresionMontosAnioMesRuta";
		args[3] = "mappers.RegressionMapper";
		args[4] = "reducers.RegressionReducer";
		args[5] = "models.RegressionVariablesWrapper";
		res = ToolRunner.run(new StartJobMain(), args);

		args[0] = "Regresion Inc. Pasajeros por dia y anio";									//REVISAR ARGS[1] (9)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaPasajerosAnioMesDiaRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/regresionIncPasajerosAnioMesDiaRuta";
		args[3] = "mappers.RegressionMapper";
		args[4] = "reducers.RegressionReducer";
		args[5] = "models.RegressionVariablesWrapper";
		res = ToolRunner.run(new StartJobMain(), args);


		args[0] = "Regresion Inc. Montos por dia y anio";									//REVISAR ARGS[1] (10)
		args[1] = "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosAnioMesDiaRuta";
		args[2] = "/user/hive/warehouse/viajesdomesticoscr.db/regresionIncMontosAnioMesDiaRuta";
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