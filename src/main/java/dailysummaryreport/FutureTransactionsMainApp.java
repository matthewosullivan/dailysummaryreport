package dailysummaryreport;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class FutureTransactionsMainApp {

	static Map<String, Integer> columnWidth = new LinkedHashMap<>();
	
	static Map<String, String> columnType = new HashMap<>();
	
	static {
		columnWidth.put("RECORDCODE", 3);
		columnWidth.put("CLIENTTYPE", 4);
		columnWidth.put("CLIENTNUMBER", 4);
		columnWidth.put("ACCOUNTNUMBER", 4);
		columnWidth.put("SUBACCOUNTNUMBER", 4);
		columnWidth.put("OPPOSITEPARTYCODE", 6);
		columnWidth.put("PRODUCTGROUPCODE", 2);
		columnWidth.put("EXCHANGECODE", 4);
		columnWidth.put("SYMBOL", 6);
		columnWidth.put("EXPIRATIONDATE", 8);
		columnWidth.put("CURRENCYCODE", 3);
		columnWidth.put("MOVEMENTCODE", 2);
		columnWidth.put("BUYSELLCODE", 1);
		columnWidth.put("QUANTTTYLONGSIGN", 1);
		columnWidth.put("QUANTITYLONG", 10);
		columnWidth.put("QUANTITYSHORTSIGN", 1);
		columnWidth.put("QUANTITYSHORT", 10);
		columnWidth.put("EXCHBROKERFEEDEC", 12);
		columnWidth.put("EXCHBROKERFEEDC", 1);
		columnWidth.put("EXCHBROKERFEECURCODE", 3);
		columnWidth.put("CLEARINGFEEDEC", 12);
		columnWidth.put("CLEARINGFEEDC", 1);
		columnWidth.put("CLEARINGFEECURCODE", 3);
		columnWidth.put("COMMISSION", 12);
		columnWidth.put("COMMISSIONDC", 1);
		columnWidth.put("COMMISSIONCURCODE", 3);
		columnWidth.put("TRANSACTIONDATE", 8);
		columnWidth.put("FUTUREREFERENCE", 6);
		columnWidth.put("TICKETNUMBER", 6);
		columnWidth.put("EXTERNALNUMBER", 6);
		columnWidth.put("TRANSACTIONPRICEDEC", 15);
		columnWidth.put("TRADERINITIALS", 6);
		columnWidth.put("OPPOSITETRADERID", 7);
		columnWidth.put("OPENCLOSECODE", 1);
		
		columnType.put("QUANTITYLONG", "INT");
		columnType.put("QUANTITYSHORT", "INT");
	}
	
	public static void main(String[] args) {
		String inputFilename = args[0];
		// sql query for extracting report using group by and sum(QUANTITYLONG) - sum(QUANTITYSHORT) to give Total_Transaction_Amount  per client per product
		String exportCSVQuery = "select CLIENTTYPE || CLIENTNUMBER || ACCOUNTNUMBER || SUBACCOUNTNUMBER as Client_Information, EXCHANGECODE || PRODUCTGROUPCODE || SYMBOL || EXPIRATIONDATE as Product_Information, sum(QUANTITYLONG) - sum(QUANTITYSHORT) as Total_Transaction_Amount from FIXEDWIDTHFILE group by CLIENTTYPE, CLIENTNUMBER, ACCOUNTNUMBER, SUBACCOUNTNUMBER, EXCHANGECODE, PRODUCTGROUPCODE, SYMBOL, EXPIRATIONDATE";
		
		String outputFilename = "Output.csv";
		new FixedWidthFile("input.txt", columnWidth)
			.columnDataTypes(columnType)
			.exportQueryResultAsCSV(exportCSVQuery, outputFilename);
		
		
	}

}
