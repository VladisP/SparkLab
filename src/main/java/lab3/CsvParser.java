package lab3;

import org.apache.hadoop.io.Text;

public class CsvParser {

    public static String[] getColumns(Text row) {

        return row.toString().replaceAll("\"", "").split(",");
    }
}
