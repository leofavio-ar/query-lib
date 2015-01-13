/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.aguilar.querylib.sql.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import org.aguilar.querylib.sql.Query2;

/**
 *
 * @author Leonardo Favio Aguilar Ramírez
 */
public class QueryFactory {
    
    private Connection connection;
    
    /**
     * Constructor de la clase
     * @param connection Un objeto de tipo Connection para inicializar los objetos Query2
     */
    public QueryFactory(Connection connection) {
        this.connection = connection;
    }
    /**
     * Cierra la conexión que se pueda haber generado por cada objeto Query2
     * @param query Un objeto de tipo Query2
     */
    public void cerrarConexion(Query2 query) {
        try {
            connection.close();
        } catch (SQLException ex) {
            try { if (connection != null) { connection.close(); } } catch (SQLException ex1) { }
        }
    }
    /**
     * Obtiene un resultado a partir de un objeto QueryObj
     * @param queryObj
     * @return Un objeto genérico que puede contener un <code>ArrayList</code> o un <code>int</code> dependiendo del tipo de QueryObj
     * @throws UnsupportedOperationException Cuando el tipo del objeto QueryObj no es uno de <code>QueryObj.SQL_SELECT, QueryObj.SQL_INSERT, QueryObj.SQL_UPDATE, QueryObj.SQL_DELETE</code>
     */
    public Object resultado(QueryObj queryObj) throws UnsupportedOperationException {
        switch (queryObj.getTipo()) {
            case QueryObj.SQL_SELECT: return select(queryObj);
            case QueryObj.SQL_INSERT: return insert(queryObj);
            case QueryObj.SQL_UPDATE: return update(queryObj);
            case QueryObj.SQL_DELETE: return delete(queryObj);
            default: throw new UnsupportedOperationException();
        }
    }
    /**
     * Realiza una sentencia select sobre el objeto Connection de la clase con los parámetros del objeto QueryObj
     * @param queryObj
     * @return Un <code>ArrayList</code> de objetos <code>Map</code> que contienen un listado de los registros regresados por la consulta
     */
    protected ArrayList<Map> select(QueryObj queryObj) {
        Query2 query = new Query2(connection);
        ArrayList<Map> al;
        if (queryObj.getParametros().length > 0) {
            al = query.select(queryObj.getSql(), queryObj.getParametros());
        } else {
            al = query.select(queryObj.getSql());
        }
//        cerrarConexion(query);
        query = null;
        queryObj = null;
        return al;
    }
    /**
     * Realiza un registro sobre el objeto Connection de la clase con los parámetros del objeto QueryObj
     * @param queryObj
     * @return Un <code>int</code> que representa el valor numérico de la nueva llave primaria de la tabla afectada, o un valor negativo que especifica el error SQL que se pudo haber producido
     */
    protected int insert(QueryObj queryObj) {
        Query2 query = new Query2(connection);
        int r = query.insert(queryObj.getTabla(), queryObj.getColumnas(), queryObj.getValores());
//        cerrarConexion(query);
        query = null;
        queryObj = null;
        return r;
    }
    /**
     * Realiza una actualización sobre el objeto Connection de la clase con los parámetros del objeto QueryObj
     * @param queryObj
     * @return Un <code>int</code> que representa el número de los registros afectados de la tabla, o un valor negativo que especifica el error SQL que se pudo haber producido
     */
    protected int update(QueryObj queryObj) {
        Query2 query = new Query2(connection);
        int r = query.update(queryObj.getTabla(), queryObj.getColumnas(), queryObj.getValores(), queryObj.getCondiciones());
//        cerrarConexion(query);
        query = null;
        queryObj = null;
        return r;
    }
    /**
     * Realiza una eliminación sobre el objeto Connection de la clase con los parámetros del objeto QueryObj
     * @param queryObj
     * @return Un <code>int</code> que representa el número de los registros afectados de la tabla, o un valor negativo que especifica el error SQL que se pudo haber producido
     */
    protected int delete(QueryObj queryObj) {
        Query2 query = new Query2(connection);
        int r = query.delete(queryObj.getTabla(), queryObj.getCondiciones());
//        cerrarConexion(query);
        query = null;
        queryObj = null;
        return r;
    }
    
}