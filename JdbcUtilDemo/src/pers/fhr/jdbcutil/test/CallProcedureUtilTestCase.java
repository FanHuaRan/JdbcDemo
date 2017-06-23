package pers.fhr.jdbcutil.test;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import pers.fhr.jdbcutil.util.CallProcedureUtil;
import pers.fhr.jdbcutil.util.OutPutParam;

public class CallProcedureUtilTestCase {

	private static CallProcedureUtil callProcedureUtil=new CallProcedureUtil();
	// 数据库用户名
	private static final String USERNAME = "sa";
	// 用户密码
	private static final String PASSWORD = "lxp212";
	// 驱动名
	private static final String DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	// 连接字符串
	private static final String URL = "jdbc:sqlserver://manaforcewin; DatabaseName=ebs20";

	// 利用静态代码块进行驱动加载
	static {
		try {
			Class.forName(DRIVER);
		} catch (ClassNotFoundException e) {
			System.out.println("驱动加载失败");
			e.printStackTrace();
		}
	}
	@SuppressWarnings("unused")
	@Test
	public void test() throws SQLException{
		Connection connection=getConnection();
		List<Object> inputParams=new ArrayList<>();
		inputParams.add(212324);
		inputParams.add("hello4");
		inputParams.add("p部门5");
		List<OutPutParam> outPutParams=new ArrayList<>();
		OutPutParam returnParam=new OutPutParam(Types.INTEGER);
		List<List<Map<String, Object>>> result=callProcedureUtil.callProcedureQuery(connection, "mps_newdepart_std", inputParams, outPutParams,returnParam);
	}
	
	@Before
	public void setUp() throws Exception {
	}

	//@Test	
	public void testCallProcedureString() throws SQLException {
		Connection connection=getConnection();
		List<Object> inputParams=new ArrayList<>();
		inputParams.add(13);
		inputParams.add("张老师");
		List<OutPutParam> outPutParams=new ArrayList<>();
		outPutParams.add(new OutPutParam(Types.INTEGER));
		OutPutParam returnParam=new OutPutParam(Types.INTEGER);
		callProcedureUtil.callProcedure(connection, "getTeacherByNameAndAge", inputParams, outPutParams,returnParam);
		//callProcedureUtil.callProcedure(connection, "getTeacherByNameAndAge", inputParams, outPutParams,null);
		assertEquals(1,outPutParams.get(0).getValue());
	}
	//testCallProcedureInt
	//@SuppressWarnings("unused")
	//@Test
	public void testCallProcedureQuery() throws SQLException {
		Connection connection=getConnection();
		List<Object> inputParams=new ArrayList<>();
		inputParams.add(13);
		inputParams.add("张老师");
		List<OutPutParam> outPutParams=new ArrayList<>();
		outPutParams.add(new OutPutParam(Types.INTEGER));
		OutPutParam returnParam=new OutPutParam(Types.INTEGER);
		List<List<Map<String, Object>>> result=callProcedureUtil.callProcedureQuery(connection, "getTeacherByNameAndAgeAndReturn", inputParams, outPutParams,returnParam);
		//List<List<Map<String, Object>>> result2=callProcedureUtil.callProcedureQuery(connection, "getTeacherByNameAndAgeAndReturn", inputParams, outPutParams,null);
		assertEquals(1,outPutParams.get(0).getValue());
		assertEquals(2, result.size());
	}
	/**
	 * 获取连接对象
	 * @return
	 * @throws SQLException 
	 */
	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(URL, USERNAME, PASSWORD);
	}

}
