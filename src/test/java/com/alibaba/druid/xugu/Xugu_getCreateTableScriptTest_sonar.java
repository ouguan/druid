package com.alibaba.druid.xugu;

import com.alibaba.druid.DbTestCase;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.JdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wsy on 29/12/2018.
 */
public class Xugu_getCreateTableScriptTest_sonar extends DbTestCase {

	private static final Logger Logger = LoggerFactory.getLogger(Xugu_getCreateTableScriptTest_sonar.class);

	public Xugu_getCreateTableScriptTest_sonar() {
		super("pool_config/xugu_tddl.properties");
	}

	public void test_oracle() {
		Connection conn;
		try {
			conn = getConnection();

			List<String> tables = JdbcUtils.showTables(conn, JdbcConstants.XUGU);
			for (String table : tables) {
				Object cnt = JdbcUtils.executeQuery(conn, "select count(*) CNT from " + table, Collections.emptyList())
						.get(0).get("CNT");
				Logger.info(table + " : " + cnt);
			}

			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Logger.error("ErrorCode: " + e.getErrorCode() + "===>" + e.getMessage());
		}
	}
}
