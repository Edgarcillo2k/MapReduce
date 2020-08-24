import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class Simulation {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    /**
     * @param args
     * @throws SQLException
     */
    static {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = null;
        try {
            con = DriverManager.getConnection("jdbc:hive2://25.83.236.39:10000/ViajesDomesticosCR", "", "");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        Statement stmt = null;
        try {
            stmt = con.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try {
            stmt.execute("CREATE TABLE Demanda(" +
                    "origen string," +
                    "destino string," +
                    "mes int," +
                    "demanda float" +
                    ")" +
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';" +
                    "INSERT OVERWRITE TABLE Demanda(origen,destino,mes,demanda)" +
                    "select compras.origen,compras.destino,compras.mes,(compras.cantidad*compras.cantidad)/(busquedas.cantidad*300)" +
                    "from ComprasXrutaXmes compras" +
                    "inner join busquedasXrutaXmes busquedas" +
                    "on (compras.origen = busquedas.origen and compras.destino = busquedas.destino and compras.mes = busquedas.mes)" +
                    "group by compras.origen,compras.destino,compras.mes" +
                    "order by compras.origen,compras.destino,compras.mes");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println("Database userdb created successfully.");
        try {
            con.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


        // show tables
        /*
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        */

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
        /*
        String filepath = "/tmp/a.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

         */
    }
};