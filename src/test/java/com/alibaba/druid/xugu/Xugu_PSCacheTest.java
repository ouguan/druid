package com.alibaba.druid.xugu;

import com.alibaba.druid.DbTestCase;
import com.alibaba.druid.util.JdbcUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wsy on 29/12/2018.
 */
public class Xugu_PSCacheTest extends DbTestCase {

	private static final Logger Logger = LoggerFactory.getLogger(Xugu_PSCacheTest.class);

	public Xugu_PSCacheTest() {
		super("pool_config/xugu_tddl.properties");
	}

	public void test_oracle() throws Exception {
		for (int i = 0; i < 1000; ++i) {
			Logger.info(i + " : -----------------------------");
			Connection conn = null;
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			try {
				conn = getConnection();
				pstmt = conn.prepareStatement("select * from t1 where f0 = ?");
				pstmt.setString(1, "3");
				rs = pstmt.executeQuery();
				JdbcUtils.printResultSet(rs);
			} catch (SQLException ex) {
				Logger.error("ErrorCode: " + ex.getErrorCode() + "===>" + ex.getMessage());
			} finally {
				JdbcUtils.close(rs);
				JdbcUtils.close(pstmt);
				JdbcUtils.close(conn);
			}

			Thread.sleep(3000);
		}
	}
}
