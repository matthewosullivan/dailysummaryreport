package dailysummaryreport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FixedWidthFile {

	private static final Logger logger = Logger.getLogger(FixedWidthFile.class.getName());

	private String connectionUrl = "jdbc:h2:mem:test";
	private Connection con;
	private Map<String, Integer> columnNameWidth;
	private String filename;

	public FixedWidthFile(String filename, Map<String, Integer> columnNameWidth) {
		this.filename = filename;
		this.columnNameWidth = columnNameWidth;

		try {
			con = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		buildTable();
		
	}

	public FixedWidthFile columnDataTypes(Map<String,String> columnTypes) {
		
		for (Entry<String, String> entry : columnTypes.entrySet()) {
			executeDDL("ALTER TABLE FIXEDWIDTHFILE ALTER COLUMN " + entry.getKey() + " " + entry.getValue() + ";");
		}
		
		//executeDDL("ALTER TABLE FIXEDWIDTHFILE ALTER COLUMN QUANTITYLONG INT;");
		//executeDDL("ALTER TABLE FIXEDWIDTHFILE ALTER COLUMN QUANTITYSHORT INT;");
		return this;
	}

	private String buildColumns() {
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
		return columns.substring(0, columns.length() - 2);
	}

	private void buildTable() {
		StringBuilder createTableFromCSVDDL = new StringBuilder();
		createTableFromCSVDDL.append("CREATE TABLE FIXEDWIDTHFILE AS SELECT ");

		createTableFromCSVDDL.append(buildColumns());

		createTableFromCSVDDL.append(" FROM CSVREAD('");
		createTableFromCSVDDL.append(filename);
		createTableFromCSVDDL.append("','ROW');");

		logger.log(Level.INFO, createTableFromCSVDDL.toString());

		executeDDL(createTableFromCSVDDL.toString());

	}

	private void executeDDL(String statement) {
		try (Statement stmt = con.createStatement();) {
			stmt.execute(statement);
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Unable to execute DDL statement", e);
			throw new RuntimeException(e);
		}
	}
	
	public FixedWidthFile exportQueryResultAsCSV(String exportCSVQuery, String outputFilename) {
		executeDDL("CALL CSVWRITE('" + outputFilename + "', '" + exportCSVQuery + "');");
		return this;
	}
	

}
