package com.rogers.cdc.api.mutations;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;






/*
public class RegularRow implements Serializable{

	private Map<String, Column> columns; 
	public RegularRow(){
		columns = new HashMap();
	}
	public static class RowVal{
		String name;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public Column getCol() {
			return col;
		}
		public void setCol(Column col) {
			this.col = col;
		}
		Column col;
		public RowVal(String _name, Column _col){
			name = _name;
			col = _col;
		}
		
	}
	
	public RegularRow(Map<String, Column> cols){
		columns = cols;
	}
	public RegularRow(RegularRow.RowVal... cols){
		this();
    	for(RegularRow.RowVal entry: cols) {  
    		 this.addColumn(entry.getName(), entry.getCol());
    	}
	}
	public void addColumn(String name, Column col){
		//System.out.print(name);
		columns.put(name, col);	
	}
	public Map<String, Column> getColumns(){
		return columns;
	}
	public Collection<Column> getRawColumns(){
		return columns.values();
	}
	Column getColumn(String name){
		return columns.get(name);
	}
    @Override
	public String toString(){
    	final StringBuilder sb = new StringBuilder();
    	for (Map.Entry<String,Column> entry : columns.entrySet()) {
            sb.append("\n ").append(entry.getKey()).append(": ").append(entry.getValue().getValue()).append(",");
        }
    	return sb.toString();
	}

    @Override
    public boolean equals(Object ob) {
        if (ob == null) return false;
        if (ob.getClass() != getClass()) return false;
        if (ob == this) return true;
 
  	   RegularRow other = (RegularRow)ob;
  	   Map<String, Column> m1 = this.columns;
  	   Map<String, Column> m2 = other.columns;
  	   if (m1.size() != m2.size())
	      return false;
	   for (String key: m1.keySet())
	      if (!m1.get(key).equals(m2.get(key)))
	         return false;
	   return true;
    }
    public Set<Map.Entry<String,Object>> entrySet(){
    	Set<Map.Entry<String,Object>> set = new HashSet();
		for (Map.Entry<String,Column> entry: columns.entrySet()) {
			set.add(new  AbstractMap.SimpleEntry<String,Object>(entry.getKey() ,entry.getValue().getValue()));
          }
		return set; 
    }
	

}
*/