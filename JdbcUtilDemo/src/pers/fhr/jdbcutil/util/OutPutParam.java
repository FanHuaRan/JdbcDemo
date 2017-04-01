package pers.fhr.jdbcutil.util;

import java.io.Serializable;
/**
 * 输出参数封装 应用于存储过程输出参数和返回值
 * @author fhr
 * @date 2017/03/24
 */
public class OutPutParam implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -267188819412044549L;
	
	private int sqlType;
	private Object value;

	public OutPutParam(int sqlType) {
		super();
		this.sqlType = sqlType;
	}

	public int getSqlType() {
		return sqlType;
	}

	public void setSqlType(int sqlType) {
		this.sqlType = sqlType;
	}

	public Object getValue() {
		return value;
	}

	void setValue(Object value) {
		this.value = value;
	}
	
}
