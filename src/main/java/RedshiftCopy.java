import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class RedshiftCopy {
  public static void main(String[] args) {
    new RedshiftCopy().run();
  }

  private void run() {
    System.out.println("Starting up...");

    String user = "<YOUR_USERNAME_HERE>";
    String password = "<YOUR_PASSWORD_HERE>";
    String hostnameAndPort = "<YOUR_REDSHIFT_CLUSTER_HERE>"; // Ex: "test.blahwhatever.us-east-1.redshift.amazonaws.com:5439"
    String database = "<YOUR_DATABASE_HERE>";
    String schema = "<YOUR_SCHEMA_HERE>";
    String tableToCopyInto = "<YOUR_TABLE_HERE>";
    String s3PathToCsv = "<YOUR_S3_PATH_HERE>";
    String iamRole = "<YOUR_IAM_ROLE_HERE>";

    Connection conn = null;
    Statement statement = null;
    try {
      Class.forName("org.postgresql.Driver");

      Properties props = new Properties();
      props.setProperty("user", user);
      props.setProperty("password", password);

      System.out.println("\n\nAttempting to connect!\n\n");
      conn = DriverManager.getConnection("jdbc:postgresql://" + hostnameAndPort + "/" + database, props);

      System.out.println("\n\nConnection made!\n\n");

      statement = conn.createStatement();
      String command = "COPY " + schema + "." + tableToCopyInto + " FROM 's3://" + s3PathToCsv + "' iam_role '" + iamRole + "' csv";

      System.out.println("\n\nExecuting...\n\n");

      statement.executeUpdate(command);

      System.out.println("\n\nHey I think it worked!!!\n\n");

      statement.close();
      conn.close();
    } catch(Exception ex) {
      System.out.println(ex.getMessage());
    }
  }
}
