package com.alibaba.druid.xugu;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.druid.DbTestCase;

public class XuguJdbcMetaDataTest extends DbTestCase {
	private static final Logger Logger = LoggerFactory.getLogger(XuguJdbcMetaDataTest.class);

	public XuguJdbcMetaDataTest() {
		super("pool_config/xugu_tddl.properties");
	}

	public String testGetJDBCVersion(Connection conn) {
		String ret = "";
		try {
			if(conn == null) {
				conn = getConnection();
			}
			ret = conn.getMetaData().getDriverName() + " " + conn.getMetaData().getDriverVersion();
			Logger.info(conn.getMetaData().getDriverName() + " " + conn.getMetaData().getDriverVersion());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			ret = e.getMessage();
			Logger.error("ErrorCode: " + e.getErrorCode() + "===>" + e.getMessage());
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					Logger.error(e.getMessage(), e);
				}
			}
		}
		return ret;
	}
}
