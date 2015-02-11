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
 * @author  Leo Aguilar
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
            Logger.getLogger(Query2.class.getName()).log(Level.SEVERE, null, ex);
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
            DatabaseMetaData meta = conexion.getMetaData();
            Map infoTabla = obtenerTiposColumna(meta, nombreTabla);
            SQLTypeMap stm = new SQLTypeMap();
            ps = conexion.prepareStatement(updateString);
            for (int i = 0; i < valores.length; i ++) {
                colocarValor(ps, i + 1, valores[i], stm.toSQLType(infoTabla.get(columnas[i]).toString()));
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
    private String obtenerPK(DatabaseMetaData metaData, String tabla) {
        try {
            ResultSet rs = metaData.getPrimaryKeys(null, null, tabla);
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
            return "";
        } catch (SQLException ex) {
            Logger.getLogger(Query2.class.getName()).log(Level.SEVERE, null, ex);
            return "";
        }
    }
    private Map obtenerTiposColumna(DatabaseMetaData metaData, String tabla) {
        Map infoTabla = new LinkedHashMap();
        try {
            ResultSet rs = metaData.getColumns(null, null, tabla, null);
            while (rs.next()) {
                infoTabla.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
            }
        } catch (SQLException ex) {
            Logger.getLogger(Query2.class.getName()).log(Level.SEVERE, null, ex);
        }
        return infoTabla;
    }
    public int ejecutarSentencia(String sql) {
        PreparedStatement ps = null;
        try {
            ps = conexion.prepareStatement(sql);
            return ps.execute() ? 1 : 2;
        } catch (SQLException ex) {
            Logger.getLogger(Query2.class.getName()).log(Level.SEVERE, null, ex);
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
            DatabaseMetaData meta = conexion.getMetaData();
            String pk = columnaRetorno.equals("") ? obtenerPK(meta, nombreTabla) : columnaRetorno;
            boolean esPgSQL = meta.getDriverName().contains("postgresql");
            Map infoTabla = obtenerTiposColumna(meta, nombreTabla);
            SQLTypeMap stm = new SQLTypeMap();
            String insertString =
//                "insert into " + nombreTabla + " (" + calculateColsString(columnas) + ")" + " values (" + calculateParamsString(valores) + ")";
                "insert into " + nombreTabla + " (" + calcularColumnas(columnas) + ")" + " values (" + calcularParametros(valores) + ")" + 
                    (esPgSQL ? (!pk.equals("") ? " returning " + pk : "") : "") + ";";
            ps = conexion.prepareStatement(insertString, PreparedStatement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < valores.length; i ++) {
                colocarValor(ps, i + 1, valores[i], stm.toSQLType(infoTabla.get(columnas[i]).toString()));
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
                for (int j = 0; j < valores[i].length; j ++) {
                    colocarValor(ps, j + 1, valores[i][j]);
                }
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
        return select(cadenaConsulta, new Object[] {});
    }
    public ArrayList<Map> select(String cadenaConsulta, Object[] params) {
        if (conexion == null) {
            throw new NullPointerException(ERR_CONXINI);
        }
        ArrayList<Map> al = new ArrayList<>();
        Map reg;
        PreparedStatement ps = null;
        try {
            ps = conexion.prepareStatement(cadenaConsulta);
            for (int i = 0; i < params.length; i ++) {
                colocarValor(ps, i + 1, params[i]);
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                reg = new LinkedHashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i ++) {
                    Object valor;
                    if (rsmd.getColumnType(i) == Types.BIGINT && rsmd.getColumnDisplaySize(i) == 1 && rsmd.getPrecision(i) == 1) {
                        valor = rs.getBoolean(i);
                    } else {
                        valor = rs.getObject(i);
                    }
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
        } catch (SQLException | IOException ex) {
            System.out.println(ex.getMessage());
            return -5;
        } finally {
            if(cs != null) { try { cs.close(); } catch(Exception ex) { } }
        }
    }
    private void colocarValor(PreparedStatement st, int i, Object valor) throws SQLException, IOException {
        colocarValor(st, i, valor, java.sql.Types.OTHER);
    }
    private void colocarValor(PreparedStatement st, int i, Object valor, int tipoSQL) throws SQLException, IOException {
        if (tipoSQL == Types.BIT) {
            valor = ((Boolean)valor).booleanValue();
        }
        if (valor == null) {
            st.setNull(i, tipoSQL);
        } else if (valor instanceof String) {
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
    
    private class SQLTypeMap {
        
        private static final String CHAR        = "CHAR";
        private static final String VARCHAR     = "VARCHAR";
        private static final String LONGVARCHAR = "LONGVARCHAR";
        private static final String NUMERIC     = "NUMERIC";
        private static final String DECIMAL     = "DECIMAL";
        private static final String TINYINT     = "TINYINT";
        private static final String SMALLINT    = "SMALLINT";
        private static final String INT         = "INT";
        private static final String BIGINT      = "BIGINT";
        private static final String TEXT        = "TEXT";
        private static final String BIT         = "BIT";
        private static final String DATE        = "DATE";
        private static final String DOUBLE      = "DOUBLE";
        
        public int toSQLType(String tipo) {
            int aux = Types.OTHER;
            switch(tipo) {
                case CHAR:
                case VARCHAR:
                case LONGVARCHAR:
                case TEXT:
                    aux = Types.VARCHAR;
                    break;
                case NUMERIC:
                    aux = Types.NUMERIC;
                    break;
                case DECIMAL:
                    aux = Types.DECIMAL;
                    break;
                case TINYINT:
                    aux = Types.TINYINT;
                    break;
                case SMALLINT:
                    aux = Types.SMALLINT;
                    break;
                case INT:
                    aux = Types.INTEGER;
                    break;
                case BIGINT:
                    aux = Types.BIGINT;
                    break;
                case BIT:
                    aux = Types.BIT;
                    break;
                case DATE:
                    aux = Types.DATE;
                    break;
                case DOUBLE:
                    aux = Types.DOUBLE;
            }
            return aux;
        }
        
        public Class<?> toClass(int tipo) {
            Class<?> aux = Object.class;

            switch (tipo) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    aux = String.class;
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    aux = java.math.BigDecimal.class;
                    break;
                case Types.BIT:
                    aux = Boolean.class;
                    break;
                case Types.TINYINT:
                    aux = Byte.class;
                    break;
                case Types.SMALLINT:
                    aux = Short.class;
                    break;
                case Types.INTEGER:
                    aux = Integer.class;
                    break;
                case Types.BIGINT:
                    aux = Long.class;
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    aux = Float.class;
                    break;
                case Types.DOUBLE:
                    aux = Double.class;
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    aux = Byte[].class;
                    break;
                case Types.DATE:
                    aux = java.sql.Date.class;
                    break;
                case Types.TIME:
                    aux = java.sql.Time.class;
                    break;
                case Types.TIMESTAMP:
                    aux = java.sql.Timestamp.class;
                    break;
            }
            return aux;
        }
    }
    
}