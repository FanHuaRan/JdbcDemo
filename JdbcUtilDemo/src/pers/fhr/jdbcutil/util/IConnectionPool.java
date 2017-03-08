package pers.fhr.jdbcutil.util;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;
/**
 * 重新包装DataSource接口 毕竟我们是要做个简易版的
 * @author FHR
 *
 */
public interface IConnectionPool extends DataSource {
	/**
	 * add methods
	 * @return
	 */
	void initialPool() throws SQLException;
	ConnectionProxy getCouldUseConnectionProxy() throws SQLException;
	void releaseConnection(Connection connection) throws SQLException;
	/**
	 * default methods
	 */
	default Connection getConnection(String username, String password) throws SQLException {
		return getConnection();
	}
	
	default PrintWriter getLogWriter() throws SQLException {
		return null;
	}

	default int getLoginTimeout() throws SQLException {
		return 0;
	}

	default Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

	default void setLogWriter(PrintWriter out) throws SQLException {

	}

	default void setLoginTimeout(int seconds) throws SQLException {
		
	}

	default boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	default <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}
}
