package pers.fhr.jdbcutil.model;

public class DataTable {
	/**
	 * Fields
	 */
	private String tableName;
	private DataRowCollection rowCollection;
	private DataColumnCollection columnCollection;
	
	/**
	 * getters setters
	 */
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public DataRowCollection getRowCollection() {
		return rowCollection;
	}
	public DataColumnCollection getColumnCollection() {
		return columnCollection;
	}
	
	/**
	 * constructor
	 */
	public DataTable(String tableName,DataRowCollection rowCollection,DataColumnCollection columnCollection){
		this.tableName=tableName;
		this.columnCollection=columnCollection;
		this.rowCollection=rowCollection;
	}
	public DataTable(String tableName,DataColumnCollection columnCollection){
		this(tableName, new DataRowCollection(), columnCollection);
	}
	public DataTable(String tableName){
		this(tableName, new DataRowCollection(), new DataColumnCollection());
	}
	public DataTable(){
		this("defaulttable",new DataRowCollection(), new DataColumnCollection());
	}
	/**
	 * methods
	 */
	public void addColumn(DataColumn column){
		this.columnCollection.add(column);
		this.rowCollection.addFiled(column.getColumnName());
	}
	public void removeColumn(DataColumn column){
		this.columnCollection.remove(column);
		this.rowCollection.removeFiled(column.getColumnName());
	}
	public void removeColumn(int index){
		DataColumn column=this.columnCollection.get(index);
		this.columnCollection.remove(index);
		this.rowCollection.removeFiled(column.getColumnName());
	}
	public DataColumn getColumn(int index){
		return columnCollection.get(index);
	}
	public DataColumn getColumn(String name){
		return columnCollection
				.stream()
				.filter(p->p.getColumnName().equals(name))
				.findFirst()
				.get();
	}
	public void setColumnName(int index,String columnName) {
		columnCollection.get(index).setColumnName(columnName);
	}
	public DataRow getRow(int index){
		return rowCollection.get(index);
	}
	public void addRow(DataRow dataRow){
		this.rowCollection.add(dataRow);
	}
	public void removeRow(int index){
		this.rowCollection.remove(index);
	}
	public void removeRow(DataRow dataRow){
		this.rowCollection.remove(dataRow);
	}
	public DataRow newRow(){
		DataRow row=new DataRow();
		this.rowCollection.add(row);
		return row;
	}
}
