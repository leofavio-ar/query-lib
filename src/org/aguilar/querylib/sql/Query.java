package org.aguilar.querylib.sql;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;

/**
 *
 * @author Leo Aguilar
 */
@Deprecated
public class Query {

    private static java.sql.Connection connection;
    private static final String ERR_ARRLONG = "La longitud de los nombres de columna es diferente a la de los valores";
    private static final String ERR_CONXINI = "La conexión no ha sido inicializada";
    private static final String ERR_ARRVALS = "El arreglo de valores no puede ser nulo";

    public Query() {
    }
    public Query(String driverName, String url, String userName, String password) {
        setConnection(driverName, url, userName, password);
    }
    public static void setConnection(String driverName, String url, String userName, String password) {
        try {
            Class.forName(driverName);
            connection = java.sql.DriverManager.getConnection(url, userName, password);
        } catch (Exception ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public static void setConnection(java.sql.Connection connection) {
        Query.connection = connection;
    }
    public static java.sql.Connection getConnection() {
        return connection;
    }
    public int update(String tableName,  String[] colNames, Object[] values, Object[][] conditions) {
        if (colNames.length != values.length)
            throw new IllegalArgumentException(ERR_ARRLONG);
        if (connection == null)
            throw new NullPointerException(ERR_CONXINI);
        PreparedStatement ps = null;
        String update =
                "update " + tableName + " set " + calculatePairs(colNames) + " " + calculateConditions(conditions) + ";";
        try {
            ps = connection.prepareStatement(update);
            for (int i = 0; i < values.length; i ++)
                setStValue(ps, i + 1, values[i]);
            for (int j = 0; j < conditions.length; j ++)
                setStValue(ps, j + values.length + 1, conditions[j][1]);
            return ps.executeUpdate();
        } catch (SQLException ex) {
            System.err.println(ex.getErrorCode() + SQLError.mysqlToXOpen(ex.getErrorCode()));
            return ex.getErrorCode() * -1;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return -1;
        } finally {
            if (ps != null) try { ps.close(); } catch(Exception e) { }
        }
    }
    public int delete(String tableName, Object[][] conditions) {
        PreparedStatement ps = null;
        String delete = "delete from " + tableName + " " + calculateConditions(conditions) + ";";
        try {
            ps = connection.prepareStatement(delete);
            for (int i = 0; i < conditions.length; i ++)
                setStValue(ps, i + 1, conditions[i][1]);
            return ps.executeUpdate();
        } catch (Exception ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        }
    }
    private String getPK(String table) {
        try {
            DatabaseMetaData dbmd = connection.getMetaData();
            ResultSet rs = dbmd.getPrimaryKeys(null, null, table);
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
            return "";
        } catch (SQLException ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            return "";
        }
    }
//<editor-fold defaultstate="collapsed" desc="INSERT">
    private int doInsert(String tableName, String[] colNames, Object[] values, String returningColumn) {
        if (colNames.length != values.length)
            throw new IllegalArgumentException(ERR_ARRLONG);
        if (connection == null)
            throw new NullPointerException(ERR_CONXINI);
        PreparedStatement ps = null;
        try {
            String pk = returningColumn.equals("") ? getPK(tableName) : returningColumn;
            String insert =
                "insert into " + tableName + " (" + calculateColsString(colNames) + ")" + " values (" + calculateParamsString(values) + ")";
                //"insert into " + tableName + " (" + calculateColsString(colNames) + ")" + " values (" + calculateParamsString(values) + ")" + (!pk.equals("") ? " returning " + pk : "") + ";";
            ps = connection.prepareStatement(insert, PreparedStatement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < values.length; i ++)
                setStValue(ps, i + 1, values[i]);
            ResultSet rs;
            if (!pk.equals("")) {
                /*
                 * POSTGRESQL
                 */
                //rs = ps.executeQuery();
                /*
                 * MYSQL
                 */
                ps.executeUpdate();
                rs = ps.getGeneratedKeys();
                if (rs.next())
                    return rs.getInt(1);
            }
            else {
                ps.execute();
                return 0;
            }
            return -2;
        } catch (IOException ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        } catch (SQLException ex) {
            System.err.println(ex.getErrorCode() + SQLError.mysqlToXOpen(ex.getErrorCode()));
            return ex.getErrorCode() * -1;
        } finally {
            if (ps != null) try { ps.close(); } catch(Exception e) { }
        }
    }
    public int insert(String tableName, String[] colNames, Object[] values, String returningColumn) {
        return doInsert(tableName, colNames, values, returningColumn);
    }
    public int insert(String tableName, String[] colNames, Object[] values) {
        return doInsert(tableName, colNames, values, "");
    }
    public void batchInsert(String tableName, String[] colNames, ArrayList<Map> values) {
        if (values.isEmpty())
            throw new IllegalArgumentException(ERR_ARRVALS);
        if (colNames.length != values.get(0).size())
            throw new IllegalArgumentException(ERR_ARRLONG);
        if (connection == null)
            throw new NullPointerException(ERR_CONXINI);
        Object[][] data = new Object[values.size()][values.get(0).size()];
        for (int row = 0; row < values.size(); row ++) {
            Iterator it = values.get(row).entrySet().iterator();
            int col = 0;
            while (it.hasNext()) {
                Entry entry = ((Entry)it.next());
                data[row][col ++] = entry.getValue();
            }
        }
        batchInsert(tableName, colNames, data);
    }
    public void batchInsert(String tableName, String[] colNames, Object[][] values) {
        if (values.length == 0)
            throw new IllegalArgumentException(ERR_ARRVALS);
        if (colNames.length != values[0].length)
            throw new IllegalArgumentException(ERR_ARRLONG);
        if (connection == null)
            throw new NullPointerException(ERR_CONXINI);
        PreparedStatement ps = null;
        try {
            String insert = "insert into " + tableName + " (" + calculateColsString(colNames) + ")" + " values (" + calculateParamsString(values[0]) + ");";
            ps = connection.prepareStatement(insert);
            for (int i = 0; i < values.length; i ++) {
                for (int j = 0; j < values[i].length; j ++)
                    setStValue(ps, j + 1, values[i][j]);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            if (ps != null) try { ps.close(); } catch(Exception e) { }
        }
    }
//</editor-fold>
//<editor-fold defaultstate="collapsed" desc="SELECT">
    public ArrayList<Map> selectTable(String tableName) {
        return select("select * from " + tableName);
    }
    public ArrayList<Map> select(String sqlQuery) {
        if (connection == null)
            throw new NullPointerException(ERR_CONXINI);
        ArrayList<Map> al = new ArrayList<Map>();
        Map reg;
        Statement st = null;
        try {
            st = connection.createStatement();
            ResultSet rs = st.executeQuery(sqlQuery);
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                reg = new LinkedHashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                    Object value = rs.getObject(i);
                    if (value instanceof InputStream) {
                        BufferedImage img = ImageIO.read((InputStream)value);
                        reg.put(rsmd.getColumnLabel(i), img);
                    }
                    else
                        reg.put(rsmd.getColumnLabel(i), value);
                }
                al.add(reg);
            }
            return al;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        } finally {
            if (st!= null) try { st.close(); } catch(Exception e) { }
        }
    }
    public ArrayList<Map> select(String sqlQuery, Object[] params) {
        if (connection == null)
            throw new NullPointerException(ERR_CONXINI);
        ArrayList<Map> al = new ArrayList<Map>();
        Map reg;
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sqlQuery);
            for (int i = 0; i < params.length; i ++)
                setStValue(ps, i + 1, params[i]);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                reg = new LinkedHashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i ++)
                    reg.put(rsmd.getColumnLabel(i), rs.getObject(i));
                al.add(reg);
            }
            return al;
        } catch (SQLException ex) {
            System.err.println(SQLError.get(SQLError.mysqlToXOpen(ex.getErrorCode())));
            return null;
        } catch (Exception ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } finally {
            if (ps != null) try { ps.close(); } catch(Exception e) { }
        }
    }
//</editor-fold>
    public int call(String procedureName, Object[] params) {
        CallableStatement cs = null;
        if (connection == null)
            throw new NullPointerException(ERR_CONXINI);
        try {
            cs = connection.prepareCall(calculateCallString(procedureName, params, 0));
            for (int i = 0; i < params.length; i ++) {
                setStValue(cs, i + 2, params[i]);
            }
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            return cs.getInt(1);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return -5;
        } finally {
            if(cs != null) try { cs.close(); } catch(Exception ex) { }
        }
    }
    private void setStValue(PreparedStatement st, int i, Object value) throws SQLException, IOException {
        if (value instanceof String)
            st.setString(i, value.toString());
        else if (value instanceof Integer)
            st.setInt(i, (Integer)value);
        else if (value instanceof java.util.Date)
            st.setDate(i, new java.sql.Date(((java.util.Date)value).getTime()));
        else if (value instanceof Boolean)
            st.setBoolean(i, (Boolean)value);
        else if (value instanceof Double)
            st.setDouble(i, (Double)value);
        else if (value instanceof File) {
            FileInputStream fis = new FileInputStream((File)value);
            st.setBinaryStream(i, fis, ((File)value).length());
        }
        else if (value instanceof byte[]) {
            byte[] bytea = getBytes(value);
            st.setBinaryStream(i, new ByteArrayInputStream(bytea), bytea.length);
        } else
            st.setObject(i, value);
    }
    private void setStValue(CallableStatement st, int i, Object value) throws SQLException, IOException {
        if (value instanceof String)
            st.setString(i, value.toString());
        else if (value instanceof Integer)
            st.setInt(i, (Integer)value);
        else if (value instanceof java.util.Date)
            st.setDate(i, new java.sql.Date(((java.util.Date)value).getTime()));
        else if (value instanceof Boolean)
            st.setBoolean(i, (Boolean)value);
        else if (value instanceof Double)
            st.setDouble(i, (Double)value);
        else if (value instanceof byte[]) {
            byte[] bytea = getBytes(value);
            st.setBinaryStream(i, new ByteArrayInputStream(bytea), bytea.length);
        } else
            st.setObject(i, value);
    }
//<editor-fold defaultstate="collapsed" desc="CALCULAR">
    private String calculateColsString(String[] cols) {
        String string = "";
        for (int i = 0; i < cols.length; i ++)
            string += cols[i] + (i == cols.length - 1 ? "" : ", ");
        return string;
    }
    private String calculateParamsString(Object[] params) {
        String string = "";
        for (int i = 0; i < params.length; i ++) {
            string += "?" + (i == params.length - 1 ? "" : ", ");
        }
        return string;
    }
    private String calculateCallString(String sp, Object[] params, int outParams) {
        String call = "{? = call " + sp + "(";
        for (int i = 0; i < params.length + outParams; i ++)
            call += "?" + (i == params.length - 1 ? ")}" : ", ");
        return call;
    }
    private String calculateConditions(Object[][] conditions) {
        if (conditions.length == 0) {
            return "";
        }
        String string = "where ";
        for (int i = 0; i < conditions.length; i ++) {
            Object[] cond = conditions[i];
            string += cond[0].toString() + " = ?" + (i == conditions.length - 1 ? "" : " and ");
        }
        return string;
    }
    private String calculatePairs(String[] colNames) {
        String string = "";
        for (int i = 0; i < colNames.length; i ++)
            string += colNames[i] + " = ?" + (i == colNames.length - 1 ? "" : ", ");
        return string;
    }
    private byte[] getBytes(Object obj) throws java.io.IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        oos.close();
        bos.close();
        return bos.toByteArray();
    }
//</editor-fold>

}