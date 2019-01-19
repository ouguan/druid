package com.alibaba.druid.xugu;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.druid.DbTestCase;
import com.alibaba.druid.util.HexBin;
import com.alibaba.druid.util.JdbcUtils;

/**
 * 用于测试JDBC 批处理
 * 
 * @author Administrator
 * @date 2018/01/09
 */
public class XuguJdbcPrepareStatementBatchTest extends DbTestCase {
	private static final Logger Logger = LoggerFactory.getLogger(XuguJdbcPrepareStatementBatchTest.class);
	
	private String execSql = "CREATE TABLE %s(" 
	                       + "ERROR_CODE VARCHAR(32) not null comment '错误码'," 
			               + "ERROR_LEVEL int not null comment '错误级别'," 
	                       + "ITEM_CLASS VARCHAR(32) not null comment '资源类别'," 
			               + "ITEM_ACTION VARCHAR(32) not null comment '资源动作'," 
	                       + "ERROR_MSG VARCHAR(200) comment '错误信息'" 
			               + ")";
	private String constraint = "alter table %s add primary key(%s)";
	
	public XuguJdbcPrepareStatementBatchTest() {
		
		super("pool_config/xugu_tddl.properties");
	}

	public void testGetJDBCVersion() {
		XuguJdbcMetaDataTest meta = new XuguJdbcMetaDataTest();
		try {
			meta.testGetJDBCVersion(getConnection());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void testPreparedStatementAddBatch() {
		Statement stmt = null;
		try {
			stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(String.format("select table_name from user_tables where table_name='%s'", "EXECSQL"));
			if(rs.next()) {
				Logger.info("表已存在！");
				stmt.executeQuery(String.format("drop table %s", "EXECSQL"));
			} else {
				// 创建表
				stmt.executeQuery(String.format(execSql, "EXECSQL"));
				stmt.executeQuery(String.format(constraint, "EXECSQL","ERROR_CODE"));
			}
			JdbcUtils.close(rs);
			
	        
			rs = getConnection().getMetaData().getColumns(null, null, "EXECSQL", null);
			//JdbcUtils.printResultSet(rs);
			
			Map<String, Object> data = new HashMap<String, Object>();
			while(rs.next()) {
				data.put(rs.getString("COLUMN_NAME"), buildData(rs.getInt("DATA_TYPE"), rs.getInt("COLUMN_SIZE")));
			}
            JdbcUtils.insertToTable(getConnection(), "EXECSQL", data);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Logger.error(e.getMessage(), e);
		} finally {
			JdbcUtils.close(stmt);
		}
	}
	
	Object buildData(int type, int size) throws SQLException {
		Object ret;
		switch (type) {
		case Types.CHAR:
		case Types.VARCHAR:
			ret = getRandomString(size);
			break;
		case Types.DATE:
			ret = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
			break;
		case Types.BOOLEAN:
			ret = false;
			break;
		case Types.TINYINT:
			ret = new Random().nextInt(Byte.MAX_VALUE);
			break;
		case Types.SMALLINT:
			ret = new Random().nextInt(Short.MAX_VALUE );
			break;
		case Types.INTEGER:
			ret = new Random().nextInt(Integer.MAX_VALUE);
			break;
		case Types.BIGINT:
			ret = new Random().nextLong();
			break;
		case Types.TIMESTAMP:
			ret = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
			break;
		case Types.DECIMAL:
			ret = new BigDecimal(Math.random() * Long.MAX_VALUE);
			break;
		case Types.CLOB:
		case Types.LONGVARCHAR:
			ret = getRandomString(size);
			break;
		case Types.NULL:
			ret = null;
			break;
		default:
			Object object = new Object();

			if (object instanceof byte[]) {
				byte[] bytes = (byte[]) object;
				String value = HexBin.encode(bytes);
				ret = value;
			} else {
				ret = String.valueOf(object);
			}
			break;
		}

		return ret;
	}
	
	public static String getRandomString(int length) {
		// 产生随机数
		Random random = new Random();
		StringBuffer sb = new StringBuffer();
		// 循环length次
		for (int i = 0; i < length; i++) {
			// 产生0-2个随机数，既与a-z，A-Z，0-9三种可能
			int number = random.nextInt(3);
			long result = 0;
			switch (number) {
			// 如果number产生的是数字0；
			case 0:
				// 产生A-Z的ASCII码
				result = Math.round(Math.random() * 25 + 65);
				// 将ASCII码转换成字符
				sb.append(String.valueOf((char) result));
				break;
			case 1:
				// 产生a-z的ASCII码
				result = Math.round(Math.random() * 25 + 97);
				sb.append(String.valueOf((char) result));
				break;
			case 2:
				// 产生0-9的数字
				sb.append(String.valueOf(new Random().nextInt(10)));
				break;
			}
		}
		return sb.toString();
	}
}
