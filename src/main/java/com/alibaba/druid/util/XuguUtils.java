/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.support.logging.Log;
import com.alibaba.druid.support.logging.LogFactory;
import com.xugu.cloudjdbc.RowId;

import oracle.jdbc.OracleResultSet;
import oracle.jdbc.internal.OraclePreparedStatement;
import oracle.jdbc.xa.client.OracleXAConnection;
import oracle.sql.ROWID;

public class XuguUtils {

    private final static Log LOG = LogFactory.getLog(XuguUtils.class);

    public static XAConnection XuguXAConnection(Connection xuguConnection) throws XAException {
        return new OracleXAConnection(xuguConnection);
    }

    public static int getRowPrefetch(PreparedStatement stmt) throws SQLException {
        com.xugu.cloudjdbc.PreparedStatement xuguStmt = stmt.unwrap(com.xugu.cloudjdbc.PreparedStatement.class);
        
        if (xuguStmt == null) {
            return -1;
        }
        
        return xuguStmt.getFetchSize();
    }

    public static void setRowPrefetch(PreparedStatement stmt, int value) throws SQLException {
    	com.xugu.cloudjdbc.PreparedStatement xuguStmt = stmt.unwrap(com.xugu.cloudjdbc.PreparedStatement.class);
        if (xuguStmt != null) {
        	xuguStmt.setFetchSize(value);
        }
    }

    public static String getVersionNumber(DruidPooledConnection conn) throws SQLException {
    	com.xugu.cloudjdbc.Connection xuguConn = (com.xugu.cloudjdbc.Connection) unwrap(conn);
        return xuguConn.getMetaData().getDatabaseProductVersion();
    }

    public static com.xugu.cloudjdbc.Connection unwrap(Connection conn) throws SQLException {
        if (conn instanceof com.xugu.cloudjdbc.Connection) {
            return (com.xugu.cloudjdbc.Connection) conn;
        }

        return conn.unwrap(com.xugu.cloudjdbc.Connection.class);
    }

    public static java.sql.RowId getROWID(ResultSet rs, int columnIndex) throws SQLException {
    	com.xugu.cloudjdbc.ResultSet xuguResultSet = rs.unwrap(com.xugu.cloudjdbc.ResultSet.class);
        return xuguResultSet.getRowId(columnIndex);
    }

    private static Set<String> builtinFunctions;

    public static boolean isBuiltinFunction(String function) {
        if (function == null) {
            return false;
        }

        String function_lower = function.toLowerCase();

        Set<String> functions = builtinFunctions;

        if (functions == null) {
            functions = new HashSet<String>();
            Utils.loadFromFile("META-INF/druid/parser/xugu/builtin_functions", functions);
            builtinFunctions = functions;
        }

        return functions.contains(function_lower);
    }

    private static Set<String> builtinTables;

    public static boolean isBuiltinTable(String table) {
        if (table == null) {
            return false;
        }

        String table_lower = table.toLowerCase();

        Set<String> tables = builtinTables;

        if (tables == null) {
            tables = new HashSet<String>();
            Utils.loadFromFile("META-INF/druid/parser/xugu/builtin_tables", tables);
            builtinTables = tables;
        }

        return tables.contains(table_lower);
    }

    private static Set<String> keywords;

    public static boolean isKeyword(String name) {
        if (name == null) {
            return false;
        }

        String name_lower = name.toLowerCase();

        Set<String> words = keywords;

        if (words == null) {
            words = new HashSet<String>();
            Utils.loadFromFile("META-INF/druid/parser/xugu/keywords", words);
            keywords = words;
        }

        return words.contains(name_lower);
    }

    public static List<String> showTables(Connection conn) throws SQLException {
        List<String> tables = new ArrayList<String>();

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select table_name from user_tables");
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }

        return tables;
    }

    public static List<String> getTableDDL(Connection conn, String... tables) throws SQLException {
        return getTableDDL(conn, Arrays.asList(tables));
    }

    public static List<String> getTableDDL(Connection conn, List<String> tables) throws SQLException {
        List<String> ddlList = new ArrayList<String>();

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            String sql = "select DBMS_METADATA.GET_DDL('TABLE', TABLE_NAME) FROM user_tables";

            if (tables.size() > 0) {
                sql += "IN (";
                for (int i = 0; i < tables.size(); ++i) {
                    if (i != 0) {
                        sql += ", ?";
                    } else {
                        sql += "?";
                    }
                }
                sql += ")";
            }
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < tables.size(); ++i) {
                pstmt.setString(i + 1, tables.get(i));
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String ddl = rs.getString(1);
                ddlList.add(ddl);
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(pstmt);
        }

        return ddlList;
    }

    public static String getCreateTableScript(Connection conn) throws SQLException {
        return getCreateTableScript(conn, true, true);
    }

    public static String getCreateTableScript(Connection conn, boolean sorted, boolean simplify) throws SQLException {
        List<String> ddlList = XuguUtils.getTableDDL(conn);

        StringBuilder buf = new StringBuilder();
        for (String ddl : ddlList) {
            buf.append(ddl);
            buf.append(';');
        }

        String ddlScript = buf.toString();

        if (! (sorted || simplify)) {
            return ddlScript;
        }

        List<SQLStatement> stmtList = SQLUtils.parseStatements(ddlScript, JdbcConstants.XUGU);
        if (simplify) {
            for (Object o : stmtList) {
                if (o instanceof SQLCreateTableStatement) {
                    SQLCreateTableStatement createTableStmt = (SQLCreateTableStatement) o;
                    createTableStmt.simplify();
                }
            }
        }

        if (sorted) {
            SQLCreateTableStatement.sort(stmtList);
        }
        return SQLUtils.toSQLString(stmtList, JdbcConstants.XUGU);
    }

}
