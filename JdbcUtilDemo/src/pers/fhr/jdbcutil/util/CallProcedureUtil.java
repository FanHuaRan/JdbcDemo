package pers.fhr.jdbcutil.util;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 存储过程调用封装
 * 关系数据库通用
 * SQLSERVER支持多数据集返回
 * ORACLE单数据集
 * 拓展：可以考虑使用另外一篇日志中的DataSet来封装最终的结果
 * @author fhr
 * @date 2017/03/24
 */
public class CallProcedureUtil {

	/**
	 * 没有任何参数的存储过程调用
	 * @param connection
	 * @param procedureName
	 * @throws SQLException
	 */
	public void callProcedureNoAnyParams(Connection connection, String procedureName) throws SQLException {
		 callProcedure(connection, procedureName, null, null,null);
	}
	
	/**
	 * 只含有返回值的存储过程调用
	 * @param connection
	 * @param procedureName
	 * @param returnParam
	 * @throws SQLException
	 */
	public void callProcedureNoParams(Connection connection, String procedureName,OutPutParam returnParam) throws SQLException {
		 callProcedure(connection, procedureName, null, null,returnParam);
	}
	/**
	 * 含有返回值和输入参数的存储过程调用
	 * 不含输出参数和结果集
	 * @param connection
	 * @param procedureName
	 * @param inputParams
	 * @param returnParam
	 * @throws SQLException
	 */
	public void callProcedureNoOutParams(Connection connection, String procedureName, List<Object> inputParams,OutPutParam returnParam)
			throws SQLException {
		 callProcedure(connection, procedureName, inputParams, null,returnParam);
	}
	
	/**
	 * 含有返回值和输出参数的存储过程调用
	 * 不含输入参数和结果集
	 * @param connection
	 * @param procedureName
	 * @param outPutParams
	 * @param returnParam
	 * @throws SQLException
	 */
	public void callProcedureNoInputParams(Connection connection, String procedureName,
			List<OutPutParam> outPutParams,OutPutParam returnParam) throws SQLException {
		 callProcedure(connection, procedureName, null, outPutParams,returnParam);
	}
	
	/**
	 * 只含有结果集的存储过程调用
	 * @param connection
	 * @param procedureName
	 * @param returnParam
	 * @return
	 * @throws SQLException
	 */
	public List<List<Map<String, Object>>> callProcedureQueryNoAnyParams(Connection connection, String procedureName)
			throws SQLException {
		return callProcedureQuery(connection, procedureName, null, null,null);
	}
	/**
	 * 含有返回参数和结果集的存储过程调用
	 * @param connection
	 * @param procedureName
	 * @param returnParam
	 * @return
	 * @throws SQLException
	 */
	public List<List<Map<String, Object>>> callProcedureQueryNoParams(Connection connection, String procedureName,OutPutParam returnParam)
			throws SQLException {
		return callProcedureQuery(connection, procedureName, null, null,returnParam);
	}
	/**
	 * 含有返回参数，输入参数和结果集的存储过程调用
	 * @param connection
	 * @param procedureName
	 * @param inputParams
	 * @param returnParam
	 * @return
	 * @throws SQLException
	 */
	public List<List<Map<String, Object>>> callProcedureQueryNoOutParams(Connection connection, String procedureName,
			List<Object> inputParams,OutPutParam returnParam) throws SQLException {
		return callProcedureQuery(connection, procedureName, inputParams, null,returnParam);
	}
	/**
	 * 含有返回参数，输出参数和结果集的存储过程调用
	 * @param connection
	 * @param procedureName
	 * @param outPutParams
	 * @param returnParam
	 * @return
	 * @throws SQLException
	 */
	public List<List<Map<String, Object>>> callProcedureQueryNoInputParams(Connection connection, String procedureName,
			List<OutPutParam> outPutParams,OutPutParam returnParam) throws SQLException {
		return callProcedureQuery(connection, procedureName, null, outPutParams,returnParam);
	}
	/**
	 * 带返回参数和输入输出参数存储过程调用
	 * @param connection
	 * @param procedureName
	 * @param inputParams
	 * @param outPutParams
	 * @param returnParam
	 * @throws SQLException
	 */
	public void callProcedure(Connection connection, String procedureName, List<Object> inputParams,
			List<OutPutParam> outPutParams,OutPutParam returnParam) throws SQLException {
		// 判断是否需要返回值
		boolean isReturn = returnParam == null ? false : true;
		// 创建调用sql
		String sql = createCallProcedureSQL(procedureName, inputParams, outPutParams, isReturn);
		// 获取调用对象
		try (CallableStatement callableStatement = connection.prepareCall(sql)) {
			// 设置或者注册相关参数 返回输出参数起始索引
			int outPutStartIndex = 0;
			if (isReturn) {
				outPutStartIndex = prepareInputAndOutPutAndReturnParams(inputParams, outPutParams,
						callableStatement, returnParam);
			} else {
				outPutStartIndex = prepareInputAndOutPutParams(inputParams, outPutParams, callableStatement);
			}
			// 执行调用
			callableStatement.execute();
			// 获取返回值
			if (isReturn) {
				returnParam.setValue(callableStatement.getObject(1));
			}
			// 获取输出参数
			if (outPutParams != null) {
				for (int i = 0; i < outPutParams.size(); i++) {
					OutPutParam outPutParam = outPutParams.get(i);
					outPutParam.setValue(callableStatement.getObject(outPutStartIndex + i + 1));
				}
			}
		}
		// connection交给外部关闭
	}
	/**
	 * 带返回参数、输入输出参数、结果集的存储过程调用
	 * 必须先获取结果集 然后获取相关参数！！！
	 * @param connection
	 * @param procedureName
	 * @param inputParams
	 * @param outPutParams
	 * @param returnParam
	 * @return
	 * @throws SQLException
	 */
	public  List<List<Map<String, Object>>> callProcedureQuery(Connection connection, String procedureName,
			List<Object> inputParams, List<OutPutParam> outPutParams,OutPutParam returnParam) throws SQLException {
		// 判断是否需要返回值
		boolean isReturn = returnParam == null ? false : true;
		// 创建调用sql
		String sql = createCallProcedureSQL(procedureName, inputParams, outPutParams, isReturn);
		// 获取调用对象
		try(CallableStatement callableStatement = connection.prepareCall(sql)){
		// 设置或者注册相关参数 返回输出参数起始索引
		int outPutStartIndex = 0;
		if (isReturn) {
			outPutStartIndex = prepareInputAndOutPutAndReturnParams(inputParams, outPutParams, callableStatement,
					returnParam);
		} else {
			outPutStartIndex = prepareInputAndOutPutParams(inputParams, outPutParams, callableStatement);
		}
		// 执行查询且封装
		List<List<Map<String, Object>>> resultLists = getProcdureResults(callableStatement);
		// 获取返回值
		if (isReturn) {
			returnParam.setValue(callableStatement.getObject(1));
		}
		// 获取输出参数
		if (outPutParams != null) {
			for (int i = 0; i < outPutParams.size(); i++) {
				OutPutParam outPutParam = outPutParams.get(i);
				outPutParam.setValue(callableStatement.getObject(outPutStartIndex + i + 1));
			}
		}
		return resultLists;
		}
	}
	/**
	 * 从CallableStatement中获取存储的多个数据集
	 * @param callableStatement
	 * @return
	 * @throws SQLException
	 */
	private List<List<Map<String, Object>>> getProcdureResults(CallableStatement callableStatement)
			throws SQLException {
		List<List<Map<String, Object>>> resultLists = new ArrayList<>();
		try (ResultSet resultSet = callableStatement.executeQuery()) {
			List<Map<String, Object>> list = getListFromResultSet(resultSet);
			resultLists.add(list);
		}
		while (callableStatement.getMoreResults()) {
			try (ResultSet moreResultSet = callableStatement.getResultSet()) {
				resultLists.add(getListFromResultSet(moreResultSet));
			}
		}
		return resultLists;
	}
	/**
	 * 从resultSet中获取List<Map<String, Object>>
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 */
	private List<Map<String, Object>> getListFromResultSet(ResultSet resultSet) throws SQLException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		int cols_len = metaData.getColumnCount();
		List<Map<String, Object>> list = new ArrayList<>();
		while (resultSet.next()) {
			list.add(getSingleHashMap(resultSet, metaData, cols_len));
		}
		return list;
	}
	/**
	 * 创建调用存储过程的sql
	 * @param procedureName
	 * @param inputParams
	 * @param outPutParams
	 * @param isReturn
	 * @return
	 */
	private String createCallProcedureSQL(String procedureName, List<Object> inputParams,
			List<OutPutParam> outPutParams,boolean isReturn) {
		StringBuilder stringBuilder = new StringBuilder("");
		int paramsSize = inputParams == null ? 0 : inputParams.size();
		paramsSize += outPutParams == null ? 0 : outPutParams.size();
		for (int i = 0; i < paramsSize; i++) {
			stringBuilder.append("?");
			if (i != (paramsSize - 1)) {
				stringBuilder.append(",");
			}
		}
		String sql = null;
		if (isReturn) {
			sql = String.format("{ ?=call %s(%s)}", procedureName, stringBuilder.toString());
		} else {
			sql = String.format("{call %s(%s)}", procedureName, stringBuilder.toString());
		}
//		if (isReturn) {
//			sql = String.format("SET NOCOUNT ON ?=exec %s %s", procedureName, stringBuilder.toString());
//		} else {
//			sql = String.format("SET NOCOUNT ON exec %s %s", procedureName, stringBuilder.toString());
//		}
		return sql;
	}
    /**
     * 准备相关输入、输出和返回参数
     * @param inputParams
     * @param outPutParams
     * @param callableStatement
     * @param returnParam
     * @return
     * @throws SQLException
     */
	private int prepareInputAndOutPutAndReturnParams(List<Object> inputParams, List<OutPutParam> outPutParams,
			CallableStatement callableStatement,OutPutParam returnParam) throws SQLException {
		callableStatement.registerOutParameter(1, returnParam.getSqlType());
		if (inputParams != null) {
			for (int i = 0; i < inputParams.size(); i++) {
				callableStatement.setObject(i + 2, inputParams.get(i));
			}
		}
		int startIndex = (inputParams == null ? 0 : inputParams.size()) + 1;
		if (outPutParams != null) {
			for (int i = 0; i < outPutParams.size(); i++) {
				callableStatement.registerOutParameter(startIndex + i + 1, outPutParams.get(i).getSqlType());
			}
		}
		return startIndex;
	}
	  /**
     * 准备相关输入、输出参数
     * @param inputParams
     * @param outPutParams
     * @param callableStatement
     * @param returnParam
     * @return
     * @throws SQLException
     */
	private int prepareInputAndOutPutParams(List<Object> inputParams, List<OutPutParam> outPutParams,
			CallableStatement callableStatement) throws SQLException {
			if (inputParams != null) {
				for (int i = 0; i < inputParams.size(); i++) {
					callableStatement.setObject(i + 1, inputParams.get(i));
				}
			}
			int startIndex = (inputParams == null ? 0 : inputParams.size());
			if (outPutParams != null) {
				for (int i = 0; i < outPutParams.size(); i++) {
					callableStatement.registerOutParameter(startIndex + i + 1, outPutParams.get(i).getSqlType());
				}
			}
			return startIndex;
	}
	/**
	 * 从查询结果获取单个map 也就是一行
	 * @param resultSet
	 * @param metaData
	 * @param col_len
	 * @return
	 * @throws SQLException
	 */
	private static Map<String, Object> getSingleHashMap(ResultSet resultSet, ResultSetMetaData metaData, int col_len)
			throws SQLException {
		Map<String, Object> map = new HashMap<>();
		for (int i = 0; i < col_len; i++) {
			String cols_name = metaData.getColumnName(i + 1);
			Object cols_value = resultSet.getObject(cols_name);
			map.put(cols_name, cols_value);
		}
		return map;
	}
}
