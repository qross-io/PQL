package io.qross.jdbc;

import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.ToScala;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataAccess extends DataSource {

    public static DataAccess DEFAULT() {
        return new DataAccess(JDBC.DEFAULT());
    }
    public static DataAccess QROSS() {
        return new DataAccess(JDBC.QROSS());
    }
    public static DataAccess MEMORY() {
        return new DataAccess(DBType.Memory());
    }

    public DataAccess() {
        this.connectionName_$eq(JDBC.DEFAULT());
        this.config_$eq(JDBC.get(this.connectionName()));
    }

    public DataAccess(String connectionName) {
        this.connectionName_$eq(connectionName);
        this.config_$eq(JDBC.get(this.connectionName()));
    }

    public DataAccess(String connectionName, String databaseName) {
        this.connectionName_$eq(connectionName);
        this.databaseName_$eq(databaseName);
        this.config_$eq(JDBC.get(this.connectionName()));
    }

    public DataTable executeDataTable(String SQL, Object... values) {
        return this.executeDataTable(SQL, ToScala.ArrayToSeq(values));
    }

    public List<Map<String, Object>> executeMapList(String SQL, Object... values) {
        return this.executeJavaMapList(SQL, ToScala.ArrayToSeq(values));
    }

    public DataRow executeDataRow(String SQL, Object... values) {
        return this.executeDataRow(SQL, ToScala.ArrayToSeq(values));
    }

    public Map<String, Object> executeMap(String SQL, Object... values) {
        return this.executeJavaMap(SQL, ToScala.ArrayToSeq(values));
    }

    public <S, T> Map<S, T> executeDataMap(String SQL, Object... values) {
        Map<S, T> map = new HashMap<>();
        ResultSet rs = this.executeResultSet(SQL, values);
        try {
            while (rs.next()) {
                map.put((S) rs.getObject(1), (T) rs.getObject(2));
            }
            if (!config().dbType().equals(DBType.Presto())) {
                rs.getStatement().close();
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    public void addBatch(Object...values) {
        addBatch(Arrays.asList(values));
    }

    public void executeBatchUpdate() {
        executeBatchUpdate(true);
    }

    public List<Object> executeList(String SQL, Object... values) {
        return this.executeJavaList(SQL, ToScala.ArrayToSeq(values));
    }

    public Object executeSingleValue(String SQL, Object... values) {
        return this.executeSingleValue(SQL, ToScala.ArrayToSeq(values)).value();
    }

    public boolean executeExists(String SQL, Object... values) {
        return this.executeExists(SQL, ToScala.ArrayToSeq(values));
    }

    public ResultSet executeResultSet(String SQL, Object... values) {
        return this.executeResultSet(SQL, ToScala.ArrayToSeq(values)).get();
    }

    public int executeNonQuery(String SQL, Object... values) {
        return this.executeNonQuery(SQL, ToScala.ArrayToSeq(values));
    }
}
