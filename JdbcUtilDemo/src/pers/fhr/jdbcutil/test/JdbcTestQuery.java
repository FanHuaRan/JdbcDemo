package pers.fhr.jdbcutil.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.mysql.jdbc.PreparedStatement;

public class JdbcTestQuery {
	
	/// 数据库用户名
		private static final String USERNAME = "root";
		// 用户密码
		private static final String PASSWORD = "123";
		// 驱动名
		private static final String DRIVER = "com.mysql.jdbc.Driver";
		// 连接字符串
		private static final String URL = "jdbc:mysql://localhost:3306/musicstore";
		
		//利用静态代码块进行驱动加载
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
	public void test() throws SQLException {
		Connection connection=getConnection();
		PreparedStatement preparedStatement=(PreparedStatement) connection.prepareStatement("delete from album where  albumid=-125 ");
		try{
			System.out.println(preparedStatement.execute());
			ResultSet resultSet=preparedStatement.getResultSet();
			System.out.println(resultSet==null);
			System.out.println(preparedStatement.getMoreResults());
		//	ResultSet resultSet=preparedStatement.executeQuery();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
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
