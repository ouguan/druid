package com.alibaba.druid.xugu;

import com.alibaba.druid.DbTestCase;
import com.alibaba.druid.pool.DruidDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wsy on 29/12/2018.
 */
public class Xugu_ConnectTest extends DbTestCase {
	
	private static final Logger Logger = LoggerFactory.getLogger(Xugu_ConnectTest.class);
	
	private String url      = "jdbc:xugu://192.168.2.77:5138/SYSTEM";
	private String user     = "SYSDBA";
	private String password = "SYSDBA";
	private String driver   = "com.xugu.cloudjdbc.Driver";
	
	public Xugu_ConnectTest() {
		super("pool_config/xugu_tddl.properties");
	}
	
    public void test_0() throws Exception {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(password);
        ds.setDriverClassName(driver);

        DriverManager.getConnection(url, user, password);

        Connection conn = ds.getConnection();
        if(!conn.isClosed()) {
        	Logger.info("Set Properties Connect is Success.");
        } else {
        	Logger.info("Set Properties Connect is Failed.");
        }
        conn.close();
        ds.close();
    }

	public void test_Xugu() throws Exception {
        Connection conn = getConnection();

        // 从虚谷获取CreateTable语句列表(暂时不支持)
        //String createTableScript = JdbcUtils.getCreateTableScript(conn, JdbcConstants.XUGU);
        //Logger.info(createTableScript);

        if(!conn.isClosed()) {
        	Logger.info("DataSource Connect is Success.");
        } else {
        	Logger.info("DataSource connect is Failed.");
        }
        conn.close();
    }
}
