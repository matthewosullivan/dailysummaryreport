package dailysummaryreport;

import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

public class FixedWidthFileTest {

	static Map<String, Integer> columnWidth = new LinkedHashMap<>();

	static Map<String, String> columnType = new HashMap<>();

	static {
		columnWidth.put("CLIENT", 4);
		columnWidth.put("PRODUCT", 3);
		columnWidth.put("QTYLONG", 2);
		columnWidth.put("QTYSHORT", 2);

		columnType.put("QTYLONG", "INT");
		columnType.put("QTYSHORT", "INT");
	}

	@Test
	public void testInputOutput() throws Exception {
		String outputfilename = "testoutput.csv";
		new FixedWidthFile("classpath:testinput.txt", columnWidth).columnDataTypes(columnType).exportQueryResultAsCSV(
				"select CLIENT, PRODUCT, sum(QTYLONG) - sum(QTYSHORT) as Total_Transaction_Amount from FIXEDWIDTHFILE group by CLIENT, PRODUCT",
				outputfilename);

		Set<String> lines = Files.lines(Paths.get(outputfilename)).collect(Collectors.toSet());


		// expected lines in file
		assertTrue(lines.contains("\"NAB \",\"A1 \",\"2\""));
		assertTrue(lines.contains("\"NAB \",\"A2 \",\"-40\""));
		
		
	}

}
