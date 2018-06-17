package dailysummaryreport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class MainApp {

	static String connectionUrl = "jdbc:h2:mem:test";
	static Connection con;
	static Map<String, Integer> columnNameWidth = new LinkedHashMap<String, Integer>();

	static {
		try {
			con = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		columnNameWidth.put("RECORDCODE", 3);
		columnNameWidth.put("CLIENTTYPE", 4);
		columnNameWidth.put("CLIENTNUMBER", 4);
		columnNameWidth.put("ACCOUNTNUMBER", 4);
		columnNameWidth.put("SUBACCOUNTNUMBER", 4);
		columnNameWidth.put("OPPOSITEPARTYCODE", 6);
		columnNameWidth.put("PRODUCTGROUPCODE", 2);
		columnNameWidth.put("EXCHANGECODE", 4);
		columnNameWidth.put("SYMBOL", 6);
		columnNameWidth.put("EXPIRATIONDATE", 8);
		columnNameWidth.put("CURRENCYCODE", 3);
		columnNameWidth.put("MOVEMENTCODE", 2);
		columnNameWidth.put("BUYSELLCODE", 1);
		columnNameWidth.put("QUANTTTYLONGSIGN", 1);
		columnNameWidth.put("QUANTITYLONG", 10);
		columnNameWidth.put("QUANTITYSHORTSIGN", 1);
		columnNameWidth.put("QUANTITYSHORT", 10);
		columnNameWidth.put("EXCHBROKERFEEDEC", 12);
		columnNameWidth.put("EXCHBROKERFEEDC", 1);
		columnNameWidth.put("EXCHBROKERFEECURCODE", 3);
		columnNameWidth.put("CLEARINGFEEDEC", 12);
		columnNameWidth.put("CLEARINGFEEDC", 1);
		columnNameWidth.put("CLEARINGFEECURCODE", 3);
		columnNameWidth.put("COMMISSION", 12);
		columnNameWidth.put("COMMISSIONDC", 1);
		columnNameWidth.put("COMMISSIONCURCODE", 3);
		columnNameWidth.put("TRANSACTIONDATE", 8);
		columnNameWidth.put("FUTUREREFERENCE", 6);
		columnNameWidth.put("TICKETNUMBER", 6);
		columnNameWidth.put("EXTERNALNUMBER", 6);
		columnNameWidth.put("TRANSACTIONPRICEDEC", 15);
		columnNameWidth.put("TRADERINITIALS", 6);
		columnNameWidth.put("OPPOSITETRADERID", 7);
		columnNameWidth.put("OPENCLOSECODE", 1);

	}

	public static boolean execute(String statement) {
		try (Statement stmt = con.createStatement();) {
			return stmt.execute(statement);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static void main(String[] args) {
		String filename = "input.txt";

		// build query

		StringBuilder createTable = new StringBuilder();
		createTable.append("CREATE TABLE FUTURETRANSACTIONS AS SELECT ");

		int startingPosition = 1;
		StringBuilder columns = new StringBuilder();
		for (Entry<String, Integer> entry : columnNameWidth.entrySet()) {
			columns.append("substring(ROW, ");
			columns.append(startingPosition);
			columns.append(", ");
			columns.append(entry.getValue());
			columns.append(") AS ");
			columns.append(entry.getKey());

			columns.append(", ");
			startingPosition += entry.getValue();
		}

		createTable.append(columns.substring(0, columns.length() - 2));

		createTable.append(" FROM CSVREAD('");
		createTable.append(filename);
		createTable.append("','ROW');");

		// createTable.append("CREATE TABLE FUTURETRANSACTIONS AS SELECT substring(ROW,
		// 0, 3) RECORDCODE FROM CSVREAD('\" + filename + \"','ROW');");
		System.out.println(createTable.toString());

		// String createTable = "CREATE TABLE FUTURETRANSACTIONS AS SELECT
		// substring(ROW, 0, 3) RECORDCODE FROM CSVREAD('" + filename + "','ROW');";

		execute(createTable.toString());

		String query = "SELECT * FROM FUTURETRANSACTIONS";

		try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			
			
			if (rs.next()) {
				
				ResultSetMetaData rsmd = rs.getMetaData();
				int cols = rsmd.getColumnCount();
				for (int i = 1; i <= cols; i++) {
					String colName = rsmd.getColumnName(i);
					//System.out.println(colName + " " + rs.getString(colName));
				}
				
				
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		execute("ALTER TABLE FUTURETRANSACTIONS ALTER COLUMN QUANTITYLONG INT;");
		execute("ALTER TABLE FUTURETRANSACTIONS ALTER COLUMN QUANTITYSHORT INT;");
		
		String exportCSVQuery = "select CLIENTTYPE || CLIENTNUMBER || ACCOUNTNUMBER || SUBACCOUNTNUMBER as Client_Information, EXCHANGECODE || PRODUCTGROUPCODE || SYMBOL || EXPIRATIONDATE as Product_Information, sum(QUANTITYLONG) - sum(QUANTITYSHORT) as Total_Transaction_Amount from FUTURETRANSACTIONS group by CLIENTTYPE, CLIENTNUMBER, ACCOUNTNUMBER, SUBACCOUNTNUMBER,    EXCHANGECODE, PRODUCTGROUPCODE, SYMBOL, EXPIRATIONDATE";
		try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery(exportCSVQuery);) {
			while (rs.next()) {
				
				ResultSetMetaData rsmd = rs.getMetaData();
				int cols = rsmd.getColumnCount();
				for (int i = 1; i <= cols; i++) {
					String colName = rsmd.getColumnName(i);
					System.out.print(colName + ": <" + rs.getString(colName) + "> ");
				}
				System.out.println("");
				
				
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		execute("CALL CSVWRITE('Output.csv', '" + exportCSVQuery + "');");

	}

}
