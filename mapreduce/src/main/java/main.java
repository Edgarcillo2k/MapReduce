import models.JobWrapper;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ToolRunner;

public class main {

    public static void main(String[] args) throws Exception {
        // jobname
        // inputPath
        // outputPath
        // outputValueClass
        // mapperClass
        // reducerClass
        // concatIndexes: 1,5,7,8
        // outIndex
        // separator
        // dateIndex
        // keyConcatDateIndexes: 1,3

        //["transactionid","fechaCompra","userId","monto","origen","destino"]
        String[] jobArgs = {"Suma montos de cada dia por mes por ruta",
                "/user/hive/warehouse/viajesdomesticoscr.db/reservaciones",
                "/user/hive/warehouse/viajesdomesticoscr.db/sumaMontosDiaMesRuta",
                "org.apache.hadoop.io.FloatWritable",
                "mappers.CollectionMapper",
                "reducers.CollectionSumReducer",
                "4,5",
                "3",
                "\001",
                "1",
                "3,2"};
        executeJob(jobArgs);
        System.exit(0);
    }


    public static <OUTPUT_VALUE_CLASS extends WritableComparable> void executeJob(String[] args) throws Exception {
        int res = ToolRunner.run(new JobWrapper<OUTPUT_VALUE_CLASS>(), args);
        if(res==0) {
            System.out.println("Job successfully completed!");
        }
        else{
            System.out.println("Job failed!");
        }
    }
}
