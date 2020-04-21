
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
 
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable {

	private static final String TABLE_NAME = "user";
	private static final String CF_DEFAULT = "personal_details";
	//private static final String CF_PERSONAL = "perso_details";
	 private static final String prof_details = "prof_details";
	private static final String COLUMN_NAME = "name";
	private static final String COLUMN_CITY = "city";
	private static final String COLUMN_DESIGNATION = "desg";
	private static final String COLUMN_SALARY = "sal";

	public static HTable hTable;

	public static void main(String... args) throws IOException {

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			//addFamily
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(prof_details));
			
//			 
			// inistiating the table
			hTable = new HTable(config, TABLE_NAME);

			// put data to table
			System.out.print("Putting data into table.... ");

			Put p = new Put(Bytes.toBytes("2"));

			p.add(Bytes.toBytes(prof_details), Bytes.toBytes(COLUMN_SALARY),
					Bytes.toBytes("160000"));

			hTable.put(p);
			
			

			// table.clone();
			connection.close();

			System.out.println(" Done!");
		}
	}

 

}