package org.aguilar.querylib.sql;

/**
 *
 * @author Leo Aguilar
 * Trébol Informatica
 * http://www.trebolinformatica.com.mx
 */
import java.sql.SQLException;

public class Connection {

    public String driver, url, host, port, dbName, username, password;
    public static final int IBM_DB2 = 0;
    public static final int SQL_SERVER = 1;
    public static final int POSTGRESQL_OLD = 2;
    public static final int FIREBIRD = 4;
    public static final int INTERBASE = 8;
    public static final int SQL_SERVER_JTURBO = 16;
    public static final int SQL_SERVER_SPRINTA = 32;
    public static final int SQL_SERVER_2000 = 64;
    public static final int MYSQL = 128;
    public static final int POSTGRESQL = 256;
    public static final int SYBASE = 512;

    public Connection() {
        this.url = "";
    }
    public Connection(int driver, String url, String username, String password) throws ClassNotFoundException {
        this.driver = getClassDriver(driver);
        this.url = url;
        this.username = username;
        this.password = password;
        Class.forName(this.driver);
    }
    public Connection(int driver, String host, int port, String dbName, String username, String password) throws ClassNotFoundException {
        this.driver = getClassDriver(driver);
        this.host = host;
        this.port = String.valueOf(port);
        this.dbName = dbName;
        this.url = getURL(driver, host, port, dbName);
        this.username = username;
        this.password = password;
        Class.forName(this.driver);
    }
    public void setDriver(int driver) {
        this.driver = getClassDriver(driver);
    }
    public void setURL(String url) {
        this.url = url;
    }
    public void setHost(String host) {
        this.host = host;
    }
    public void setPort(int port) {
        this.port = String.valueOf(port);
    }
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    public void setUserName(String username) {
        this.username = username;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public Object getDriver() {
        return this.driver;
    }
    public Object getURL() {
        return this.driver;
    }
    public Object getUserName() {
        return this.username;
    }
    public Object getPassword() {
        return this.password;
    }
    public java.sql.Connection getConnection() throws ClassNotFoundException {
        try {
            Class.forName(driver);
            return java.sql.DriverManager.getConnection(url, username, password);
        }
        catch(SQLException ex) {
            return null;
        }
    }
    private String getClassDriver(int T) {
        switch (T) {
            case 0: return "COM.ibm.db2.jdbc.app.DB2Driver";
            case 1: return "weblogic.jdbc.mssqlserver4.Driver";
            case 2: return "postgresql.Driver";
            case 4: return "org.firebirdsql.jdbc.FBDriver";
            case 8: return "interbase.interclient.Driver";
            case 16: return "com.ashna.jturbo.driver.Driver";
            case 32: return "com.inet.tds.TdsDriver";
            case 64: return "com.microsoft.jdbc.sqlserver.SQLServerDriver";
            case 128: return "org.gjt.mm.mysql.Driver";
            case 256: return "org.postgresql.Driver";
            case 512: return "com.sybase.jdbc2.jdbc.SybDriver";
            default: return null;
        }
    }
    private String getURL(int T, String host, int port, String dbName) {
        switch (T) {
            case 0: return "jdbc:db2://" + host + ":" + port + "/" + dbName;
            case 1: return "jdbc:weblogic:mssqlserver4:" + dbName + "@" + host + ":" + port;
            case 2: return "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
            case 4: return "jdbc:firebirdsql://" + host + ":" + port + "/" + dbName;
            case 8: return "jdbc:interbase://" + host + "/" + dbName;
            case 16: return "jdbc:JTurbo://" + host + ":" + port + "/" + dbName;
            case 32: return "jdbc:inetdae:" + host + ":" + port + "?database=" + dbName;
            case 64: return "jdbc:microsoft:sqlserver://" + host + ":" + port + ";DatabaseName=" + dbName;
            case 128: return "jdbc:mysql://" + host + ":" + port + "/" + dbName;
            case 256: return "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
            case 512: return "jdbc:sybase:Tds:" + host + ":" + port;
            default: return null;
        }
    }

}