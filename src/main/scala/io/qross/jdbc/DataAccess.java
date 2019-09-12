package io.qross.jdbc;

import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.core.DataType;
import io.qross.time.Timer;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class DataAccess {

    public static DataAccess QROSS = new DataAccess(JDBC.QROSS());
    public static DataAccess DEFAULT = new DataAccess(JDBC.DEFAULT());
    public static DataAccess MEMORY = new DataAccess(DBType.Memory());

    private List<String> batchSQLs = new ArrayList<>();
    private List<List<Object>> batchValues = new ArrayList<>();

    private JDBC config = null;

    private String connectionName = JDBC.DEFAULT(); //current connection name

    private Connection connection = null; //current connection
    private long tick = -1; //not opened

    public DataAccess() {

    }

    public DataAccess(String connectionName) {
        this.connectionName = connectionName;
    }


    private void open() {

        config = JDBC.get(connectionName);

        try {
            Class.forName(config.driver()).newInstance();
        }
        catch(ClassNotFoundException e) {
            if (!config.alternativeDriver().equals("")) {
                try {
                    Class.forName(config.alternativeDriver()).newInstance();
                }
                catch (ClassNotFoundException e1) {
                    System.err.println("Open database ClassNotFoundException " + e.getMessage());
                }
                catch (Exception e1) {
                    System.err.println("Open database Exception with alternative driver: " + e.getMessage());
                }
            }
            else {
                System.err.println("Open database ClassNotFoundException " + e.getMessage());
            }
        }
        catch (InstantiationException e) {
            System.err.println("Open database InstantiationException " + e.getMessage());
        }
        catch (IllegalAccessException e) {
            System.err.println("Open database IllegalAccessException " + e.getMessage());
        }
        catch (Exception e) {
            System.err.println("Open database SQLException " + e.getMessage());
        }

        //尝试连接
        try {
            if (!config.username().equals("")) {
                this.connection = DriverManager.getConnection(config.connectionString(), config.username(), config.password());
            }
            else {
                this.connection = DriverManager.getConnection(config.connectionString());
            }
        }
        catch (SQLException e) {
            System.err.println("Open database SQLException " + e.getMessage());
        }

        if (config.dbType().equals(DBType.MySQL())) {
            if (this.connection != null) {
                try {
                    PreparedStatement prest = this.connection.prepareStatement("SELECT 1 AS T");
                    ResultSet rs = prest.executeQuery();
                    rs.close();
                    prest.close();
                }
                catch(Exception e) {
                    e.printStackTrace();
                    System.err.println("Test connection Exception: " + e.getMessage());
                }
            }
        }
    }

    // ---------- basic command ----------

    public DataTable executeDataTable(String SQL, Object...values) {
        DataTable table = new DataTable();
        ResultSet rs = this.executeResultSet(SQL, values);
        if (rs != null) {
            try {
                ResultSetMetaData meta = rs.getMetaData();
                int columns = meta.getColumnCount();
                String fieldName;
                for (int i = 1; i <= columns; i++) {
                    fieldName = meta.getColumnLabel(i); //alias name
                    if (fieldName.contains(".")) fieldName = fieldName.substring(fieldName.lastIndexOf(".") + 1);  // . is illegal char in SQLite
                    if (!Pattern.matches("^[a-zA-Z_][a-zA-Z0-9_]*$", fieldName) || table.contains(fieldName)) fieldName = "column" + i;
                    table.addFieldWithLabel(fieldName, meta.getColumnLabel(i), DataType.ofTypeName(meta.getColumnTypeName(i), meta.getColumnClassName(i)));
                }

                List<String> fields = table.getFieldNameList();
                while (rs.next()) {
                    DataRow row = table.newRow();
                    for (int i = 1; i <= columns; i++) {
                        row.set(fields.get(i), rs.getObject(i), table.getFieldDataType(fields.get(i)));
                    }
                    table.addRow(row);
                }
                rs.getStatement().close();
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return table;
    }

    public DataRow executeDataRow(String SQL, Object...values) {
        DataRow row = new DataRow();
        try {
            ResultSet rs = this.executeResultSet(SQL, values);
            ResultSetMetaData meta = rs.getMetaData();
            int columns = meta.getColumnCount();
            if (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    row.set(meta.getColumnLabel(i), rs.getObject(i));
                }
                rs.getStatement().close();
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return row;
    }

    public List<Map<String, Object>> executeMapList(String SQL, Object...values) {
        List<Map<String, Object>> mapList = new ArrayList<>();
        ResultSet rs = this.executeResultSet(SQL, values);
        if (rs != null) {
            try {
                ResultSetMetaData meta = rs.getMetaData();
                int columns = meta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> map = new HashMap<>();
                    for (int i = 1; i <= columns; i++) {
                        map.put(meta.getColumnLabel(i), rs.getObject(i));
                    }
                    mapList.add(map);
                }
                rs.getStatement().close();
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return mapList;
    }

    public List<String> executeSingleList(String SQL, Object...values) {
        List<String> list = new ArrayList<>();
        ResultSet rs = this.executeResultSet(SQL, values);
        if (rs != null) {
            try {
                while (rs.next()) {
                    list.add(rs.getString(1));
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public Map<String, String> executeHashMap(String SQL, Object...values) {
        Map<String, String> map = new LinkedHashMap<>();
        ResultSet rs = this.executeResultSet(SQL, values);
        if (rs != null) {
            try {
                while (rs.next()) {
                    map.put(rs.getString(1), rs.getString(2));
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    public String executeSingleValue(String SQL, Object...values) {
        String value = "";
        try {
            ResultSet rs = this.executeResultSet(SQL, values);
            if (rs.next()) {
                value = rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return value;
    }

    public Boolean executeExists(String SQL, Object...values) {
        boolean result = false;
        try {
            ResultSet rs = this.executeResultSet(SQL, values);
            if (rs.next()) {
                result = true;
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public ResultSet executeResultSet(String SQL, Object...values) {
        this.openIfNot();

        ResultSet rs = null;
        int retry = 0;
        while (rs == null) {
            try {
                PreparedStatement prest = this.connection.prepareStatement(trimSQL(SQL));
                for (int i = 0; i < values.length; i++) {
                    prest.setObject(i + 1, values[i]);
                }
                rs = prest.executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            retry++;
            if (retry >= 3) {
                break;
            }
        }
        return rs;
    }

    public int executeUpdate(String SQL, Object...values) {
        this.openIfNot();

        int row = -1;
        try {
            PreparedStatement prest = this.connection.prepareStatement(trimSQL(SQL));
            for (int i = 0; i < values.length; i++) {
                prest.setObject(i + 1,  values[i]);
            }
            row = prest.executeUpdate();
            prest.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return row;
    }

    public void addBatchCommand(String SQL) {
        this.batchSQLs.add(trimSQL(SQL));
    }

    public void setBatchCommand(String SQL) {
        this.batchSQLs.clear();
        this.batchSQLs.add(trimSQL(SQL));
    }

    public void addBatch(Object...values) {
        this.batchValues.add(Arrays.asList(values));
    }

    public void addBatch(String...values) {
        this.batchValues.add(Arrays.asList((Object[])values));
    }

    public void addBatch(List<Object> values) {
        this.batchValues.add(values);
    }

    public void executeBatchUpdate() {
        executeBatchUpdate(true);
    }

    public int executeBatchUpdate(boolean commitOnExcute) {
        this.openIfNot();

        int count = 0;
        if (!this.batchSQLs.isEmpty()) {
            if (!this.batchValues.isEmpty()) {
                try {
                    this.connection.setAutoCommit(false);
                    PreparedStatement prest = this.connection.prepareStatement(this.batchSQLs.get(0));
                    for (List<Object> values : this.batchValues) {
                        for (int i = 0; i < values.size(); i++) {
                            prest.setObject(i + 1, values.get(i));
                        }
                        prest.addBatch();

                        count++;
                        if (count % 1000 == 0) {
                            prest.executeBatch();
                            if (commitOnExcute) {
                                this.connection.commit();
                            }
                        }
                    }
                    if (count % 1000 > 0) {
                        prest.executeBatch();
                        this.connection.commit();
                    }
                    this.connection.setAutoCommit(true);
                    prest.clearBatch();
                    prest.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                this.batchValues.clear();
            }
            else {
                try {
                    this.connection.setAutoCommit(false);
                    PreparedStatement prest = this.connection.prepareStatement("");
                    for (String SQL : this.batchSQLs) {
                        prest.addBatch(SQL);

                        count++;
                        if (count % 1000 == 0) {
                            prest.executeBatch();
                            if (commitOnExcute) {
                                this.connection.commit();
                            }
                        }
                    }
                    if (count % 1000 > 0) {
                        prest.executeBatch();
                        this.connection.commit();
                    }
                    this.connection.setAutoCommit(true);
                    prest.clearBatch();
                    prest.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            this.batchSQLs.clear();
        }

        return count;
    }

    public int executeBatchInsert() {
        return executeBatchInsert(1000);
    }

    public int executeBatchInsert(int batchSize) {
        int count = 0;
        if (!this.batchSQLs.isEmpty() && !this.batchValues.isEmpty()) {
            int location;
            String baseSQL = this.batchSQLs.get(0).toUpperCase();
            if (baseSQL.contains("VALUES")) {
                location = baseSQL.indexOf("VALUES") + 6;
                baseSQL = this.batchSQLs.get(0).substring(0, location) + " ";
            }
            else {
                baseSQL = this.batchSQLs.get(0) + " VALUES ";
            }

            List<String> rows = new ArrayList<>();
            String v, vs;
            for (List<Object> values : this.batchValues) {
                vs = "('";
                for (int i = 0; i < values.size(); i++) {
                    v = String.valueOf(values.get(i));
                    if (i > 0) {
                        vs += "', '";
                    }
                    if (v.contains("'")) {
                        v = v.replace("'", "''");
                    }
                    vs += v;
                }
                vs += "')";
                rows.add(vs);

                if (rows.size() % batchSize == 0) {
                    count += this.executeUpdate(baseSQL + String.join(",", rows));
                    rows.clear();
                }
            }

            if (rows.size() > 0) {
                count += this.executeUpdate(baseSQL + String.join(",", rows));
                rows.clear();
            }

            this.batchSQLs.clear();
            this.batchValues.clear();
        }
        return count;
    }

    // --------- other ----------

    private long getIdleTime() {
        return (this.tick == -1 ? -1 : System.currentTimeMillis() - this.tick);
    }

    private void openIfNot() {
        try {
            int retry = 0;
            if (this.getIdleTime() >= 10000) {
                this.close();
            }
            if (this.tick == -1 || this.connection.isClosed()) {
                while (this.connection == null && retry < 100) {
                    this.open();
                    if (this.connection == null) {
                        Timer.sleep(0.2F);
                        retry++;
                    }
                }
            }
            if (this.connection != null) {
                this.tick = System.currentTimeMillis();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (this.connection != null && !this.connection.isClosed()) {
                this.tick = -1;
                this.connection.close();
                this.connection = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean queryTest() {
        this.open();
        boolean connected = (this.connection != null);
        this.close();

        return connected;
    }

    public DataTable queryDataTable(String SQL, Object...values) {

        DataTable dataTable = this.executeDataTable(SQL, values);
        this.close();

        return dataTable;
    }

    public DataRow queryDataRow(String SQL, Object...values) {

        DataRow dataRow = this.executeDataRow(SQL, values);
        this.close();

        return dataRow;
    }

    public int queryUpdate(String SQL, Object...values) {

        int rows = this.executeUpdate(SQL, values);
        this.close();

        return rows;
    }

    public String querySingleValue(String SQL, Object...values) {

        String value = this.executeSingleValue(SQL, values);
        this.close();

        return value;
    }

    public List<String> querySingleList(String SQL, Object...values) {

        List<String> list = this.executeSingleList(SQL, values);
        this.close();

        return list;
    }

    public Map<String, String> queryHashMap(String SQL, Object...values) {

        Map<String, String> map = this.executeHashMap(SQL, values);
        this.close();

        return map;
    }

    public boolean queryExists(String SQL, Object...values) {

        boolean exists = this.executeExists(SQL, values);
        this.close();

        return exists;
    }

    private static String trimSQL(String SQL) {
        SQL = SQL.trim();
        if (SQL.endsWith(";")) {
            SQL = SQL.substring(0, SQL.length() - 1);
        }
        return SQL;
    }
}
