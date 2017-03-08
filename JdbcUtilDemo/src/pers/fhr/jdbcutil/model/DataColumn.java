package pers.fhr.jdbcutil.model;

import java.util.List;

import junit.extensions.TestSetup;

public class DataColumn {
	//fields
	 private DataTable table; //dataTable的引用
     private String columnName; //
     private String captionName; //显示名称
     private int columnIndex;//列索引
     private int dataType;//列数据类型
	 private String dataTypeName;//数据类型名称
	 //construtor
	 public DataColumn() {
	        this("default1");
      }   
      public DataColumn(int dataType) {
	        this("default1", dataType);
	  }
      public DataColumn(String columnName) {
	        this(columnName, 0);
      }   
	  public DataColumn(String columnName, int dataType) {
	        this.setDataType(dataType);
	        this.columnName = columnName;
      }
	  // getters setters   
	  public String getColumnName(){
		return columnName;
	  }	
	  public void setColumnName(String columnName){
		  if(this.columnName.equals(columnName)){
			  return;
		  }
		  if(table!=null){
			  table.getRowCollection().updateFiledName(this.columnName, columnName);
		  }
		  this.columnName=columnName;
	  }
		public DataTable getTable() {
			return table;
		}
		public void setTable(DataTable table) {
			this.table = table;
		}
		public String getCaptionName() {
			return captionName;
		}
		public void setCaptionName(String captionName) {
			this.captionName = captionName;
		}
		public int getColumnIndex() {
			return columnIndex;
		}
		public void setColumnIndex(int columnIndex) {
			this.columnIndex = columnIndex;
		}
		public int getDataType() {
			return dataType;
		}
		public void setDataType(int dataType) {
			this.dataType = dataType;
		}
		public String getDataTypeName() {
			return dataTypeName;
		}
		public void setDataTypeName(String dataTypeName) {
			this.dataTypeName = dataTypeName;
		}
		  
		/* 功能描述：  将输入数据转为当前列的数据类型返回
		 * @param
		 */
		public Object convertTo(Object value) {
		      return value;
		 }
		       
		 @Override
		 public String toString(){
		      return this.columnName;
		 }
}
