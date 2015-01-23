/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.aguilar.querylib.sql.utils;

/**
 *
 * @author Leo Aguilar
 */
public class QueryObj {

    public static final int SQL_SELECT = 0;
    public static final int SQL_INSERT = 1;
    public static final int SQL_UPDATE = 2;
    public static final int SQL_DELETE = 3;
    private int tipo;
    private String sql;
    private Object[] parametros;
    private String[] columnas;
    private Object[] valores;
    private Object[][] condiciones;
    private String tabla;

    public QueryObj(int tipo) {
        this.tipo = tipo;
        this.sql = "";
        this.parametros = new Object[0];
        this.columnas = new String[0];
        this.valores = new Object[0];
        this.condiciones = new Object[0][0];
        this.tabla = "";
    }
    public QueryObj(int tipo, String sql, Object[] parametros, String[] columnas, Object[] valores, Object[][] condiciones, String tabla) {
        this.tipo = tipo;
        this.sql = sql;
        this.parametros = parametros;
        this.columnas = columnas;
        this.valores = valores;
        this.condiciones = condiciones;
        this.tabla = tabla;
    }
    public static QueryObj selQuery(String sql) {
        return new QueryObj(QueryObj.SQL_SELECT, sql, new Object[0], new String[0], new Object[0], new Object[0][0], "");
    }
    public static QueryObj selQuery(String sql, Object[] parametros) {
        return new QueryObj(QueryObj.SQL_SELECT, sql, parametros, new String[0], new Object[0], new Object[0][0], "");
    }
    public static QueryObj insQuery(String tabla, String[] columnas, Object[] valores) {
        return new QueryObj(QueryObj.SQL_INSERT, "", new Object[0], columnas, valores, new Object[0][0], tabla);
    }
    public static QueryObj updQuery(String tabla, String[] columnas, Object[] valores, Object[][] condiciones) {
        return new QueryObj(QueryObj.SQL_UPDATE, "", new Object[0], columnas, valores, condiciones, tabla);
    }
    public static QueryObj delQuery(String tabla, Object[][] condiciones) {
        return new QueryObj(QueryObj.SQL_DELETE, "", new Object[0], new String[0], new Object[0], condiciones, tabla);
    }
    public String getTabla() {
        return tabla;
    }
    public void setTabla(String tabla) {
        this.tabla = tabla;
    }
    public Object[] getParametros() {
        return parametros;
    }
    public void setParametros(Object[] parametros) {
        this.parametros = parametros;
    }
    public int getTipo() {
        return tipo;
    }
    public void setTipo(int tipo) {
        this.tipo = tipo;
    }
    public String getSql() {
        return sql;
    }
    public void setSql(String sql) {
        this.sql = sql;
    }
    public String[] getColumnas() {
        return columnas;
    }
    public void setColumnas(String[] columnas) {
        this.columnas = columnas;
    }
    public Object[] getValores() {
        return valores;
    }
    public void setValores(Object[] valores) {
        this.valores = valores;
    }
    public Object[][] getCondiciones() {
        return condiciones;
    }
    public void setCondiciones(Object[][] condiciones) {
        this.condiciones = condiciones;
    }
        
}
