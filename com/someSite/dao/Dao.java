package someSite.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class Dao {

    final String ALTER_TABLE = "ALTER TABLE ";
    final String ADD_COLUMNS = "ADD COLUMN ";
    final String COMMA = ", ";
    final String VARCHAR = " VARCHAR";

    final String INSERT = "INSERT INTO ";

    private StringBuilder COLUMNS;

    private String transitTableName;


    @Autowired
    JdbcTemplate jdbcTemplate;

    public void createTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + '"' + transitTableName + '"' + " " +
                "(id SERIAL PRIMARY KEY, " +
                "ISVALID BOOLEAN DEFAULT TRUE, " +
                "row_number INT);");
    }

    public void deleteTable() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS " + '"' + transitTableName + '"');
    }

    public List<String> getColumns() {
        return jdbcTemplate.queryForList("SELECT information_schema.columns.column_name" +
                " FROM information_schema.columns" +
                " WHERE table_schema = 'public'" +
                " AND table_name   = '" + transitTableName + "'" +
                "AND information_schema.columns.column_name <> 'id' " +
                "AND information_schema.columns.column_name <> 'isvalid' " +
                "AND information_schema.columns.column_name <> 'row_number' " +
                "AND information_schema.columns.column_name <> 'name'", String.class);
    }

    public void addColumns(String[][] arrayOfString) {

//        List<String> mapList = jdbcTemplate.queryForList("SELECT information_schema.columns.column_name"+
//                                                " FROM information_schema.columns" +
//                                                " WHERE table_schema = 'public'" +
//                                                " AND table_name   = 'transittable'", String.class);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(ALTER_TABLE).append('"').append(transitTableName).append('"').append(" ");
        COLUMNS = new StringBuilder();
        COLUMNS.append("row_number, ");

        // if (mapList.size() == 1){
        for (int i = 0; i < arrayOfString.length; i++) {
            stringBuilder.append(ADD_COLUMNS).append(arrayOfString[i][2]).append(VARCHAR)
            .append("(").append(arrayOfString[i][3]).append(")");
            COLUMNS.append(arrayOfString[i][2]);
            if (i != arrayOfString.length - 1) {
                stringBuilder.append(COMMA);
                COLUMNS.append(COMMA);
            }
        }

        if (stringBuilder.toString().length() > 26) {
            jdbcTemplate.execute(stringBuilder.toString());
        }

    }

    public void setTransitTableName(String transitTableName) {
        this.transitTableName = transitTableName;
    }

    public void insertData(ArrayList<ArrayList<String>> arrayValue) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(INSERT).append('"').append(transitTableName).append('"').append(" ").append("(").append(COLUMNS).append(")").append(" VALUES");
        int countArrayValue = arrayValue.size();
        for (ArrayList<String> arrayOfString : arrayValue) {
            stringBuilder.append(" (");
            int countArrayOfString = arrayOfString.size();

            for (String str : arrayOfString) {
                stringBuilder.append("'").append(str).append("'");
                countArrayOfString--;
                if (countArrayOfString != 0) {
                    stringBuilder.append(COMMA);
                }
            }
            stringBuilder.append(")");
            countArrayValue--;
            if (countArrayValue != 0) {
                stringBuilder.append(COMMA);
            }
        }
        jdbcTemplate.execute(stringBuilder.toString());
    }

    public List<Map<String, Object>> selectAll() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("select * from ").append('"').append(transitTableName).append('"').append(" WHERE isvalid = TRUE");
        return jdbcTemplate.queryForList(strBuilder.toString());
    }

    public List<Map<String, Object>> selectTransitTables() {
        return jdbcTemplate.queryForList("SELECT * FROM transittables");
    }

    public void setInToTransitTables(String tTableName) {
        List<Map<String, Object>> tTables = jdbcTemplate.queryForList("select * from transittables " +
                "where transittables.name_table = '" + tTableName + "'");
        if (tTables.isEmpty()) {
            jdbcTemplate.execute("INSERT INTO transittables (name_table) VALUES ('" + tTableName + "')");
        }
    }

    public void createTransitTables() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS transittables " +
                "(id SERIAL PRIMARY KEY, " +
                "name_table VARCHAR(255));");
    }

    public void upDateIsValidFalse(List<Map<String, Object>> failValid) {
        if (failValid == null) return;
        for (Map map : failValid) {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append("UPDATE ").append('"').append(transitTableName).append('"').append(" SET ").
                    append("isValid = false Where id = ").append(map.get("id"));

            jdbcTemplate.execute(strBuilder.toString());
        }
    }

    public List<Map<String, Object>> checkValidInFieldNameByUnique(String field) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("select TT.id, TT.row_number, TT.").append('"').append(field).append('"').append(" From ")
                .append('"').append(transitTableName).append('"').append(" TT Where TT.").append('"').append(field).append('"')
                .append(" in (Select ").append('"').append(transitTableName).append('"')
                .append(".").append('"').append(field).append('"').append(" From")
                .append('"').append(transitTableName).append('"')
                .append(" where ").append('"').append(transitTableName).append('"').append(".isvalid = true ")
                .append(" Group By ").append('"').append(transitTableName).append('"').append(".")
                .append('"').append(field).append('"').append(" Having Count(*) > 1)");

        return jdbcTemplate.queryForList(strBuilder.toString());
    }

    public List<Map<String, Object>> checkValidByNotNullField(String field) {

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("select TT.id, TT.row_number, TT.").append('"').append(field).append('"').append(" From ")
                .append('"').append(transitTableName).append('"').append(" TT Where TT.").append('"').append(field).append('"')
                .append(" is null").append(" AND TT.isvalid = true");

        return jdbcTemplate.queryForList(strBuilder.toString());
    }

    public List<Map<String, Object>> checkValidByIntField(String field) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("select TT.id , TT.row_number, TT.").append('"').append(field).append('"').append(" From ")
                .append('"').append(transitTableName).append('"').append(" TT Where TT.isvalid = true");

        return jdbcTemplate.queryForList(strBuilder.toString());
    }

    public List<Map<String, Object>> checkManualValidation(String sqlText) {
        try {
            return jdbcTemplate.queryForList(sqlText);
        } catch (Exception ex) {
            return null;
        }
    }

    public HashMap<String, Object> getReport(String head, String typeVal, Map<String, Object> resultVal, String tableName, Integer sizeColumns, Integer count) {

        HashMap<String, Object> report = new HashMap<>();
        report.put("Head", head);
        report.put("Validation sql request", typeVal);
        report.put("Table name", tableName);
        if (sizeColumns == 1) report.put("Lvl", "Field lvl");
        else report.put("Lvl", "Table lvl");

        if (resultVal == null || resultVal.size() == 0) {
            return null;
        } else {
            report.put("Count", count);
            report.putAll(resultVal);

        }
        return report;
    }

    public List<Map<String, Object>> parseVarCharToInt(List<Map<String, Object>> mapList) {
        List<Map<String, Object>> list =new ArrayList<>();
        if (mapList == null) return mapList;
        for (Map m : mapList) {
            Map<String, Object> map = new HashMap<>();
            Set set = m.keySet();
            String id = new String();
            String row_number = new String();
            for (Object srt :set) {
                if (srt.toString().equals("id")) {
                    id = m.get(srt.toString()).toString();
                    continue;
                }
                if (srt.toString().equals("row_number")) {
                    row_number = m.get(srt.toString()).toString();
                    continue;
                }
                try {
                    int tempRank = m.get(srt.toString()) == null ? null : (int) Double.parseDouble(m.get(srt.toString()).toString());
                } catch (NumberFormatException ex) {
                    System.out.println(ex.getStackTrace());
                    map = new HashMap<>();
                    map.put("id", id);
                    map.put("row_number", row_number);
                    map.put(srt.toString(), m.get(srt.toString()));
                    list.add(map);
                }
            }
        }
        return list;
    }
}
