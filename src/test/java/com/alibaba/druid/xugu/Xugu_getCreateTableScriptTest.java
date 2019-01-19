package com.alibaba.druid.xugu;

import com.alibaba.druid.DbTestCase;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.JdbcUtils;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wsy on 29/12/2018.
 */
public class Xugu_getCreateTableScriptTest extends DbTestCase {

	private static final Logger Logger = LoggerFactory.getLogger(Xugu_getCreateTableScriptTest.class);

	public Xugu_getCreateTableScriptTest() {
		super("pool_config/xugu_tddl.properties");
	}

	public void test_Xugu() {
		Connection conn = null;
		try {
			conn = getConnection();
			String createTableScript = JdbcUtils.getCreateTableScript(conn, JdbcConstants.XUGU);
			Logger.info(createTableScript);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Logger.error("ErrorCode: " + e.getErrorCode() + "===>" + e.getMessage());
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					Logger.error(e.getMessage(), e);
				}
		}
	}
}
