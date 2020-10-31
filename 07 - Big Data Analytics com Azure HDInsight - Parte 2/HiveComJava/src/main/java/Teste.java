import java.sql.*;

public class Teste {

    public static void main(String[] args) throws SQLException {

        String clusterName = "datascienceacademy";     // Coloque o nome do seu cluster
        String clusterAdmin = "admin";                 // Coloque o usuário de administração do cluster
        String clusterPassword = "MeuClusterSpark1!";  // Coloque a senha de administração do cluster

        //Variables to hold statements, connection, and results
        Connection conn = null;
        Statement stmt = null;
        Statement stmt2 = null;
        ResultSet res1 = null;
        ResultSet res2 = null;

        try
        {
            //Load the HiveServer2 JDBC driver
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            //Create the connection string
            // Note that HDInsight always uses the external port 443 for SSL secure
            //  connections, and will direct it to the hiveserver2 from there
            //  to the internal port 10001 that Hive is listening on.
            String connectionQuery = String.format(
                    "jdbc:hive2://%s.azurehdinsight.net:443/default;transportMode=http;ssl=true;httpPath=/hive2",
                    clusterName);

            //Get the connection using the cluster admin user and password
            conn = DriverManager.getConnection(connectionQuery,clusterAdmin,clusterPassword);
            stmt = conn.createStatement();

            //Will be reused for qeuries and results
            String sql =null;

            //Retrieve data from the table
            sql = "SELECT querytime, market, deviceplatform, devicemodel, state, country from hivesampletable LIMIT 3";
            stmt2 = conn.createStatement();
            System.out.println("\nRetrieving inserted data:");

            res2 = stmt2.executeQuery(sql);

            while (res2.next()) {
                System.out.println( res2.getString(1) + "\t" + res2.getString(2) + "\t" + res2.getString(3) + "\t" + res2.getString(4) + "\t" + res2.getString(5) + "\t" + res2.getString(6));
            }
            System.out.println("\nHive queries completed successfully!");
        }

        //Catch exceptions
        catch (SQLException e )
        {
            e.getMessage();
            e.printStackTrace();
            System.exit(1);
        }
        catch(Exception ex)
        {
            ex.getMessage();
            ex.printStackTrace();
            System.exit(1);
        }
        //Close connections
        finally {
            if (res1!=null) res1.close();
            if (res2!=null) res2.close();
            if (stmt!=null) stmt.close();
            if (stmt2!=null) stmt2.close();
        }
    }

}
