package cn.qross.core;

import cn.qross.jdbc.JDBC;

public class DataCube {

    public static DataCube DEFAULT = new DataCube(JDBC.DEFAULT());
    public static DataCube QROSS = new DataCube(JDBC.QROSS());

    public DataCube() {

    }

    public DataCube(String defaultConnectionName) {

    }


    public DataCube openDefault() {
        return this;
    }

    public DataCube openQross() {
        return this;
    }

    public DataCube openCache() {
        return this;
    }

    public DataCube openTemp() {
        return this;
    }

    public DataCube open(String connectionName) {
        return this;
    }

    public DataCube saveAsDefault() {
        return this;
    }

    public DataCube saveAsQross() {
        return this;
    }

    public DataCube saveAsCache() {
        return this;
    }

    public DataCube saveAsTemp() {
        return this;
    }

    public DataCube saveAs(String connectionName) {
        return this;
    }

    public DataCube reset() {
        return this;
    }

    public DataCube cache(String tableName) {
        return this;
    }

    public DataCube cache(String tableName, String primaryKey) {
        return this;
    }

    public DataCube cache(String tableName, DataTable table) {
        return this;
    }

    public DataCube cache(String tableName, DataTable table, String primaryKey) {
        return this;
    }

    public DataCube temp(String tableName) {
        return this;
    }

    public DataCube temp(String tableName, String...keys) {
        return this;
    }

    public DataCube temp(String tableName, DataTable table) {
        return this;
    }

    public DataCube temp(String tableName, DataTable table, String...keys) {
        return this;
    }

    public DataCube set(String tableName, Object...values) {
        return this;
    }

    public DataCube select(String selectSQL, Object...values) {
        return this;
    }

    public DataCube get(String selectSQL, Object...values) {
        return this;
    }

    public DataCube join(String selectSQL, String...on) {
        return this;
    }

    public DataCube pass(String selectSQL, String...defaultValues) {
        return this;
    }
}
