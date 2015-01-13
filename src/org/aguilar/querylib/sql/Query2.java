/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;

/**
 *
 * @author  ISC Leonardo Aguilar
 *          Caja Solidaria Sierra de San Juan SC de AP de RL de CV
 *          Jefatura de Sistemas e Información
 */
public class Query2 {

    private java.sql.Connection conexion;
    private static final String ERR_ARRLONG = "La longitud de los nombres de columna es diferente a la de los valores";
    private static final String ERR_CONXINI = "La conexión no ha sido inicializada";
    private static final String ERR_ARRVALS = "El arreglo de valores no puede ser nulo";

    public Query2() {
        
    }
    public Query2(String driver, String url, String usuario, String clave) {
        setConexion(driver, url, usuario, clave);
    }
    public Query2(java.sql.Connection conexion) {
        setConexion(conexion);
    }
    public void cerrarConexion() throws SQLException {
        conexion.close();
    }
    public void setConexion(String driver, String url, String usuario, String clave) {
        try {
            Class.forName(driver);
            conexion = java.sql.DriverManager.getConnection(url, usuario, clave);
        } catch (Exception ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void setConexion(java.sql.Connection connection) {
        this.conexion = connection;
    }
    public java.sql.Connection getConexion() {
        return conexion;
    }
    public int update(String nombreTabla,  String[] columnas, Object[] valores, Object[][] condiciones) {
        if (columnas.length != valores.length) {
            throw new IllegalArgumentException(ERR_ARRLONG);
        }
        if (conexion == null) {
            throw new NullPointerException(ERR_CONXINI);
        }
        PreparedStatement ps = null;
        String updateString =
                "update " + nombreTabla + " set " + calcularPares(columnas) + " " + calcularCondiciones(condiciones) + ";";
        try {
            ps = conexion.prepareStatement(updateString);
            for (int i = 0; i < valores.length; i ++) {
                colocarValor(ps, i + 1, valores[i]);
            }
            for (int j = 0; j < condiciones.length; j ++) {
                colocarValor(ps, j + valores.length + 1, condiciones[j][1]);
            }
            return ps.executeUpdate();
        } catch (SQLException ex) {
            System.err.println(ex.getErrorCode() + SQLError.mysqlToXOpen(ex.getErrorCode()));
            return ex.getErrorCode() * -1;
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            return -1;
        } finally {
            if (ps != null) { try { ps.close(); } catch(Exception e) { } }
        }
    }
    public int eliminarTabla(String nombreTabla) {
        PreparedStatement ps = null;
        String delete = "delete from " + nombreTabla + ";";
        try {
            ps = conexion.prepareStatement(delete);
            return ps.executeUpdate();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            return -1;
        }
    }
    public int delete(String nombreTabla, Object[][] condiciones) {
        PreparedStatement ps = null;
        String delete = "delete from " + nombreTabla + " " + calcularCondiciones(condiciones) + ";";
        try {
            ps = conexion.prepareStatement(delete);
            for (int i = 0; i < condiciones.length; i ++) {
                colocarValor(ps, i + 1, condiciones[i][1]);
            }
            return ps.executeUpdate();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            return -1;
        }
    }
    private String obtenerPK(String tabla) {
        try {
            DatabaseMetaData dbmd = conexion.getMetaData();
            ResultSet rs = dbmd.getPrimaryKeys(null, null, tabla);
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
            return "";
        } catch (SQLException ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            return "";
        }
    }
    public int ejecutarSentencia(String sql) {
        PreparedStatement ps = null;
        try {
            ps = conexion.prepareStatement(sql);
            return ps.execute() ? 1 : 2;
        } catch (SQLException ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        }
    }
//<editor-fold defaultstate="collapsed" desc="INSERT">
    private int doInsert(String nombreTabla, String[] columnas, Object[] valores, String columnaRetorno) {
        if (columnas.length != valores.length) {
            throw new IllegalArgumentException(ERR_ARRLONG);
        }
        if (conexion == null) {
            throw new NullPointerException(ERR_CONXINI);
        }
        PreparedStatement ps = null;
        try {
            String pk = columnaRetorno.equals("") ? obtenerPK(nombreTabla) : columnaRetorno;
            DatabaseMetaData meta = conexion.getMetaData();
            boolean esPgSQL = meta.getDriverName().contains("postgresql");
            String insertString =
//                "insert into " + nombreTabla + " (" + calculateColsString(columnas) + ")" + " values (" + calculateParamsString(valores) + ")";
                "insert into " + nombreTabla + " (" + calcularColumnas(columnas) + ")" + " values (" + calcularParametros(valores) + ")" + 
                    (esPgSQL ? (!pk.equals("") ? " returning " + pk : "") : "") + ";";
            ps = conexion.prepareStatement(insertString, PreparedStatement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < valores.length; i ++) {
                colocarValor(ps, i + 1, valores[i]);
            }
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
                if (rs.next()) {
                    return rs.getInt(1);
                }
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
            System.err.println(ex.getErrorCode() + "-" + SQLError.mysqlToXOpen(ex.getErrorCode()) + ": " + ex.getMessage());
            return ex.getErrorCode() * -1;
        } finally {
            if (ps != null) { try { ps.close(); } catch(Exception e) { } }
        }
    }
    public int insert(String nombreTabla, String[] columnas, Object[] valores, String columnaRetorno) {
        return doInsert(nombreTabla, columnas, valores, columnaRetorno);
    }
    public int insert(String nombreTabla, String[] columnas, Object[] valores) {
        return doInsert(nombreTabla, columnas, valores, "");
    }
    public void batchInsert(String nomobreTabla, String[] columnas, ArrayList<Map> valores) {
        if (valores.isEmpty()) {
            throw new IllegalArgumentException(ERR_ARRVALS);
        }
        if (columnas.length != valores.get(0).size()) {
            throw new IllegalArgumentException(ERR_ARRLONG);
        }
        if (conexion == null) {
            throw new NullPointerException(ERR_CONXINI);
        }
        Object[][] datos = new Object[valores.size()][valores.get(0).size()];
        for (int row = 0; row < valores.size(); row ++) {
            Iterator it = valores.get(row).entrySet().iterator();
            int col = 0;
            while (it.hasNext()) {
                Map.Entry entry = ((Map.Entry)it.next());
                datos[row][col ++] = entry.getValue();
            }
        }
        batchInsert(nomobreTabla, columnas, datos);
    }
    public void batchInsert(String nombreTabla, String[] columnas, Object[][] valores) {
        if (valores.length == 0) {
            throw new IllegalArgumentException(ERR_ARRVALS);
        }
        if (columnas.length != valores[0].length) {
            throw new IllegalArgumentException(ERR_ARRLONG);
        }
        if (conexion == null) {
            throw new NullPointerException(ERR_CONXINI);
        }
        PreparedStatement ps = null;
        try {
            String insertString = "insert into " + nombreTabla + " (" + calcularColumnas(columnas) + ")" + " values (" + calcularParametros(valores[0]) + ");";
            ps = conexion.prepareStatement(insertString);
            for (int i = 0; i < valores.length; i ++) {
                for (int j = 0; j < valores[i].length; j ++)
                    colocarValor(ps, j + 1, valores[i][j]);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        } finally {
            if (ps != null) { try { ps.close(); } catch(Exception e) { } }
        }
    }
//</editor-fold>
//<editor-fold defaultstate="collapsed" desc="SELECT">
    public ArrayList<Map> seleccionarTabla(String nombreTabla) {
        return select("select * from " + nombreTabla);
    }
    public ArrayList<Map> select(String cadenaConsulta) {
        if (conexion == null)
            throw new NullPointerException(ERR_CONXINI);
        ArrayList<Map> al = new ArrayList<Map>();
        Map reg;
        Statement st = null;
        try {
            st = conexion.createStatement();
            ResultSet rs = st.executeQuery(cadenaConsulta);
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                reg = new LinkedHashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                    Object valor = rs.getObject(i);
                    if (valor instanceof InputStream) {
                        BufferedImage img = ImageIO.read((InputStream)valor);
                        reg.put(rsmd.getColumnLabel(i), img);
                    } else {
                        reg.put(rsmd.getColumnLabel(i), valor);
                    }
                }
                al.add(reg);
            }
            return al;
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            return null;
        } finally {
            if (st!= null) { try { st.close(); } catch(Exception e) { } }
        }
    }
    public ArrayList<Map> select(String sqlQuery, Object[] params) {
        if (conexion == null) {
            throw new NullPointerException(ERR_CONXINI);
        }
        ArrayList<Map> al = new ArrayList<Map>();
        Map reg;
        PreparedStatement ps = null;
        try {
            ps = conexion.prepareStatement(sqlQuery);
            for (int i = 0; i < params.length; i ++) {
                colocarValor(ps, i + 1, params[i]);
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                reg = new LinkedHashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                    reg.put(rsmd.getColumnLabel(i), rs.getObject(i));
                }
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
            if (ps != null) { try { ps.close(); } catch(Exception e) { } }
        }
    }
//</editor-fold>
    public int call(String nombreProcAlmacenado, Object[] parametros) {
        CallableStatement cs = null;
        if (conexion == null) {
            throw new NullPointerException(ERR_CONXINI);
        }
        try {
            cs = conexion.prepareCall(calcularParametrosSP(nombreProcAlmacenado, parametros, 0));
            for (int i = 0; i < parametros.length; i ++) {
                colocarValor(cs, i + 2, parametros[i]);
            }
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            return cs.getInt(1);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return -5;
        } finally {
            if(cs != null) { try { cs.close(); } catch(Exception ex) { } }
        }
    }
    private void colocarValor(PreparedStatement st, int i, Object valor) throws SQLException, IOException {
        if (valor instanceof String) {
            st.setString(i, valor.toString());
        } else if (valor instanceof Integer) {
            st.setInt(i, (Integer)valor);
        } else if (valor instanceof java.util.Date) {
            st.setDate(i, new java.sql.Date(((java.util.Date)valor).getTime()));
        } else if (valor instanceof Boolean) {
            st.setBoolean(i, (Boolean)valor);
        } else if (valor instanceof Double) {
            st.setDouble(i, (Double)valor);
        } else if (valor instanceof File) {
            FileInputStream fis = new FileInputStream((File)valor);
            st.setBinaryStream(i, fis, ((File)valor).length());
        } else if (valor instanceof byte[]) {
            byte[] bytea = getBytes(valor);
            st.setBinaryStream(i, new ByteArrayInputStream(bytea), bytea.length);
        } else {
            st.setObject(i, valor);
        }
    }
    private void colocarValor(CallableStatement st, int i, Object valor) throws SQLException, IOException {
        if (valor instanceof String) {
            st.setString(i, valor.toString());
        } else if (valor instanceof Integer) {
            st.setInt(i, (Integer)valor);
        } else if (valor instanceof java.util.Date) {
            st.setDate(i, new java.sql.Date(((java.util.Date)valor).getTime()));
        } else if (valor instanceof Boolean) {
            st.setBoolean(i, (Boolean)valor);
        } else if (valor instanceof Double) {
            st.setDouble(i, (Double)valor);
        } else if (valor instanceof byte[]) {
            byte[] bytea = getBytes(valor);
            st.setBinaryStream(i, new ByteArrayInputStream(bytea), bytea.length);
        } else {
            st.setObject(i, valor);
        }
    }
//<editor-fold defaultstate="collapsed" desc="CALCULAR">
    private String calcularColumnas(String[] cols) {
        String string = "";
        for (int i = 0; i < cols.length; i ++) {
            string += cols[i] + (i == cols.length - 1 ? "" : ", ");
        }
        return string;
    }
    private String calcularParametros(Object[] params) {
        String string = "";
        for (int i = 0; i < params.length; i ++) {
            string += "?" + (i == params.length - 1 ? "" : ", ");
        }
        return string;
    }
    private String calcularParametrosSP(String sp, Object[] params, int outParams) {
        String call = "{? = call " + sp + "(";
        for (int i = 0; i < params.length + outParams; i ++) {
            call += "?" + (i == params.length - 1 ? ")}" : ", ");
        }
        return call;
    }
    private String calcularCondiciones(Object[][] conditions) {
        String string = "where ";
        for (int i = 0; i < conditions.length; i ++) {
            Object[] cond = conditions[i];
            string += cond[0].toString() + " = ?" + (i == conditions.length - 1 ? "" : " and ");
        }
        return string;
    }
    private String calcularPares(String[] colNames) {
        String string = "";
        for (int i = 0; i < colNames.length; i ++) {
            string += colNames[i] + " = ?" + (i == colNames.length - 1 ? "" : ", ");
        }
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