package lab3;

class CsvParser {

    static String[] getColumns(String row) {

        return row.replaceAll("\"", "").split(",");
    }
}
