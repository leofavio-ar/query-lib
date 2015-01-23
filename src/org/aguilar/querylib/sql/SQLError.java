package org.aguilar.querylib.sql;


import java.util.Hashtable;


/**
 * SQLError is a utility class that maps MySQL error codes to X/Open
 * error codes as is required by the JDBC spec.
 *
 * @author Mark Matthews <mmatthew_at_worldserver.com> (Translated by Leo Aguilar)
 * @version $Id: SQLError.java,v 1.2 2002/04/21 03:03:46 mark_matthews Exp $
 */
public class SQLError {

    //~ Instance/static variables .............................................

    private static Hashtable mysqlToSqlState;
    private static Hashtable sqlStateMessages;

    //~ Initializers ..........................................................

    static {
        sqlStateMessages = new Hashtable();
        sqlStateMessages.put("01002", "Error de desconexión"); //Disconnect error
        sqlStateMessages.put("01004", "Datos truncados"); //Data truncated
        sqlStateMessages.put("01006", "Privilegio no revocado"); //Privilege not revoked
        sqlStateMessages.put("01S00", "Atributo de cadena de conexión inválido"); //Invalid connection string attribute
        sqlStateMessages.put("01S01", "Error en renglón"); //Error in row
        sqlStateMessages.put("01S03", "Ningún renglón actualizado o eliminado"); //No rows updated or deleted
        sqlStateMessages.put("01S04", "Más de un renglón actualizado o eliminado"); //More than one row updated or deleted
        sqlStateMessages.put("07001", "Número de parámetros equivocado"); //Wrong number of parameters
        sqlStateMessages.put("08001", "Imposible conectar al origen de datos"); //Unable to connect to data source
        sqlStateMessages.put("08002", "Conexión en uso"); //Connection in use
        sqlStateMessages.put("08003", "Conexión no abierta"); //Connection not open
        sqlStateMessages.put("08004", "El origen de datos rechazó la conexión"); //Data source rejected establishment of connection
        sqlStateMessages.put("08007", "La conexión falló durante la transacción"); //Connection failure during transaction
        sqlStateMessages.put("08S01", "Falló el enlace de comunicación"); //Communication link failure
        sqlStateMessages.put("21S01", "La lista de valores a insertar no coincide con la lista de las columnas"); //Insert value list does not match column list
        sqlStateMessages.put("22003", "Valor numérico fuera de rango"); //Numeric value out of range
        sqlStateMessages.put("22005", "Valor numérico fuera de rango"); //Numeric value out of range
        sqlStateMessages.put("22008", "Desbordamiento de campo de hora/fecha"); //Datetime field overflow
        sqlStateMessages.put("22012", "División sobre cero"); //Division by zero
        sqlStateMessages.put("28000", "Especificación de autorización inválida"); //Invalid authorization specification
        sqlStateMessages.put("42000", "Error de sintaxis o violación de acceso"); //Syntax error or access violation
        sqlStateMessages.put("S0001", "La tabla base o vista ya existe"); //Base table or view already exists
        sqlStateMessages.put("S0002", "Tabla base no encontrada"); //Base table not found
        sqlStateMessages.put("S0011", "El índice ya existe"); //Index already exists
        sqlStateMessages.put("S0012", "Índice no encontrado"); //Index not found
        sqlStateMessages.put("S0021", "La columna ya existe"); //Column already exists
        sqlStateMessages.put("S0022", "Columna no encontrada"); //Column not found
        sqlStateMessages.put("S0023", "Columna sin default"); //No default for column
        sqlStateMessages.put("S1000", "Error general"); //General error
        sqlStateMessages.put("S1001", "Falló la asignación de memoria"); //Memory allocation failure
        sqlStateMessages.put("S1002", "Número de columna inválido"); //Invalid column number
        sqlStateMessages.put("S1009", "Valor de argumento inválido"); //Invalid argument value
        sqlStateMessages.put("S1C00", "Controlador no capaz"); //Driver not capable
        sqlStateMessages.put("S1T00", "Tiempo de espera expirado"); //Timeout expired

        //
        // Map MySQL error codes to X/Open error codes
        //
        mysqlToSqlState = new Hashtable();

        //
        // Communications Errors
        //
        // ER_BAD_HOST_ERROR 1042
        // ER_HANDSHAKE_ERROR 1043
        // ER_UNKNOWN_COM_ERROR 1047
        // ER_IPSOCK_ERROR 1081
        //
        mysqlToSqlState.put(new Integer(1042), "08S01");
        mysqlToSqlState.put(new Integer(1043), "08S01");
        mysqlToSqlState.put(new Integer(1047), "08S01");
        mysqlToSqlState.put(new Integer(1081), "08S01");

        //
        // Authentication Errors
        //
        // ER_ACCESS_DENIED_ERROR 1045
        //
        mysqlToSqlState.put(new Integer(1045), "28000");

        //
        // Resource errors
        //
        // ER_CANT_CREATE_FILE 1004
        // ER_CANT_CREATE_TABLE 1005
        // ER_CANT_LOCK 1015
        // ER_DISK_FULL 1021
        // ER_CON_COUNT_ERROR 1040
        // ER_OUT_OF_RESOURCES 1041
        //
        // Out-of-memory errors
        //
        // ER_OUTOFMEMORY 1037
        // ER_OUT_OF_SORTMEMORY 1038
        //
        mysqlToSqlState.put(new Integer(1037), "S1001");
        mysqlToSqlState.put(new Integer(1038), "S1001");

        //
        // Syntax Errors
        //
        // ER_PARSE_ERROR 1064
        // ER_EMPTY_QUERY 1065
        //
        mysqlToSqlState.put(new Integer(1064), "42000");
        mysqlToSqlState.put(new Integer(1065), "42000");

        //
        // Invalid argument errors
        //
        // ER_WRONG_FIELD_WITH_GROUP 1055
        // ER_WRONG_GROUP_FIELD 1056
        // ER_WRONG_SUM_SELECT 1057
        // ER_TOO_LONG_IDENT 1059
        // ER_DUP_FIELDNAME 1060
        // ER_DUP_KEYNAME 1061
        // ER_DUP_ENTRY 1062
        // ER_WRONG_FIELD_SPEC 1063
        // ER_NONUNIQ_TABLE 1066
        // ER_INVALID_DEFAULT 1067
        // ER_MULTIPLE_PRI_KEY 1068
        // ER_TOO_MANY_KEYS 1069
        // ER_TOO_MANY_KEY_PARTS 1070
        // ER_TOO_LONG_KEY 1071
        // ER_KEY_COLUMN_DOES_NOT_EXIST 1072
        // ER_BLOB_USED_AS_KEY 1073
        // ER_TOO_BIG_FIELDLENGTH 1074
        // ER_WRONG_AUTO_KEY 1075
        // ER_NO_SUCH_INDEX 1082
        // ER_WRONG_FIELD_TERMINATORS 1083
        // ER_BLOBS_AND_NO_TERMINATED 1084
        //
        mysqlToSqlState.put(new Integer(1055), "S1009");
        mysqlToSqlState.put(new Integer(1056), "S1009");
        mysqlToSqlState.put(new Integer(1057), "S1009");
        mysqlToSqlState.put(new Integer(1059), "S1009");
        mysqlToSqlState.put(new Integer(1060), "S1009");
        mysqlToSqlState.put(new Integer(1061), "S1009");
        mysqlToSqlState.put(new Integer(1062), "S1009");
        mysqlToSqlState.put(new Integer(1063), "S1009");
        mysqlToSqlState.put(new Integer(1066), "S1009");
        mysqlToSqlState.put(new Integer(1067), "S1009");
        mysqlToSqlState.put(new Integer(1068), "S1009");
        mysqlToSqlState.put(new Integer(1069), "S1009");
        mysqlToSqlState.put(new Integer(1070), "S1009");
        mysqlToSqlState.put(new Integer(1071), "S1009");
        mysqlToSqlState.put(new Integer(1072), "S1009");
        mysqlToSqlState.put(new Integer(1073), "S1009");
        mysqlToSqlState.put(new Integer(1074), "S1009");
        mysqlToSqlState.put(new Integer(1075), "S1009");
        mysqlToSqlState.put(new Integer(1082), "S1009");
        mysqlToSqlState.put(new Integer(1083), "S1009");
        mysqlToSqlState.put(new Integer(1084), "S1009");

        //
        // ER_WRONG_VALUE_COUNT 1058
        //
        mysqlToSqlState.put(new Integer(1058), "21S01");

        // ER_CANT_CREATE_DB 1006
        // ER_DB_CREATE_EXISTS 1007
        // ER_DB_DROP_EXISTS 1008
        // ER_DB_DROP_DELETE 1009
        // ER_DB_DROP_RMDIR 1010
        // ER_CANT_DELETE_FILE 1011
        // ER_CANT_FIND_SYSTEM_REC 1012
        // ER_CANT_GET_STAT 1013
        // ER_CANT_GET_WD 1014
        // ER_UNEXPECTED_EOF 1039
        // ER_CANT_OPEN_FILE 1016
        // ER_FILE_NOT_FOUND 1017
        // ER_CANT_READ_DIR 1018
        // ER_CANT_SET_WD 1019
        // ER_CHECKREAD 1020
        // ER_DUP_KEY 1022
        // ER_ERROR_ON_CLOSE 1023
        // ER_ERROR_ON_READ 1024
        // ER_ERROR_ON_RENAME 1025
        // ER_ERROR_ON_WRITE 1026
        // ER_FILE_USED 1027
        // ER_FILSORT_ABORT 1028
        // ER_FORM_NOT_FOUND 1029
        // ER_GET_ERRNO 1030
        // ER_ILLEGAL_HA 1031
        // ER_KEY_NOT_FOUND 1032
        // ER_NOT_FORM_FILE 1033
        // ER_DBACCESS_DENIED_ERROR 1044
        // ER_NO_DB_ERROR 1046
        // ER_BAD_NULL_ERROR 1048
        // ER_BAD_DB_ERROR 1049
        // ER_TABLE_EXISTS_ERROR 1050
        // ER_BAD_TABLE_ERROR 1051
        // ER_NON_UNIQ_ERROR 1052
        // ER_BAD_FIELD_ERROR 1054
        mysqlToSqlState.put(new Integer(1054), "S0022");

        // ER_TEXTFILE_NOT_READABLE 1085
        // ER_FILE_EXISTS_ERROR 1086
        // ER_LOAD_INFO 1087
        // ER_ALTER_INFO 1088
        // ER_WRONG_SUB_KEY 1089
        // ER_CANT_REMOVE_ALL_FIELDS 1090
        // ER_CANT_DROP_FIELD_OR_KEY 1091
        // ER_INSERT_INFO 1092
        // ER_INSERT_TABLE_USED 1093
    }

    //~ Methods ...............................................................

    public static String get(String stateCode) {

        return (String) sqlStateMessages.get(stateCode);
    }

    /**
   * Map MySQL error codes to X/Open error codes
   *
   * @param errno the MySQL error code
   * @return the corresponding X/Open error code
   */
    public static String mysqlToXOpen(int errno) {

        Integer err = new Integer(errno);

        if (mysqlToSqlState.containsKey(err)) {

            return (String) mysqlToSqlState.get(err);
        } else {

            return "S1000";
        }
    }
}