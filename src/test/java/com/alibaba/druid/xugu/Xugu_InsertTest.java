package com.alibaba.druid.xugu;

import com.alibaba.druid.DbTestCase;
import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.Statement;

public class Xugu_InsertTest  extends DbTestCase {
    public Xugu_InsertTest() {
        super("pool_config/mysql_oracle_info.properties");
    }
    public void test_for_mysql() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        stmt.execute("use oracle_info");

        stmt.close();
        conn.close();
    }
}
