
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
			
			System.out.print("Creating table.... ");
			
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			
			System.out.println(" Done!");
			// inistiating the table
			hTable = new HTable(config, TABLE_NAME);

			// put data to table
			System.out.print("Putting data into table.... ");

			List<List<String>> data = new ArrayList<List<String>>();

			List<String> rows = new ArrayList<String>() {
				{
					add("1");
					add("2");
					add("3");
				}
			};

			data.add(new ArrayList<String>() {
				{
					add("John");
					add("Boston");
					add("Manager");
					add("150000");
				}
			});
			data.add(new ArrayList<String>() {
				{
					add("Mary");
					add("New York");
					add("Sr. Engineer");
					add("130000");
				}
			});
			data.add(new ArrayList<String>() {
				{
					add("Bob");
					add("Fremont");
					add("Jr. Engineer");
					add("90000");
				}
			});

			addUsers(rows, data);

			// table.clone();
			connection.close();

			System.out.println(" Done!");
		}
	}

	public static void addUsers(List<String> row, List<List<String>> data)
			throws IOException {
		int i = 0;
		for (List<String> user : data) {
			Put p = new Put(Bytes.toBytes(row.get(i)));

			p.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COLUMN_NAME),
					Bytes.toBytes(user.get(0)));

			p.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COLUMN_CITY),
					Bytes.toBytes(user.get(1)));

			p.add(Bytes.toBytes(prof_details),
					Bytes.toBytes(COLUMN_DESIGNATION),
					Bytes.toBytes(user.get(2)));

			p.add(Bytes.toBytes(prof_details), Bytes.toBytes(COLUMN_SALARY),
					Bytes.toBytes(user.get(3)));

			hTable.put(p);
			++i;
		}
	}

}