package com.rogers.cdc.api.mutations;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;

// TODO: Clean this up...
// We're converting Row to Kafka connect struct... why not just use Struct to represent columns? 
// Struct assumes a schema is present, which is not really the case if when using GenericAvroMutationSerializer/DeSerializer (it produces string values with no Schema...), we still want to support both. 
// Mayebe create an Abstact Row with SchemaRow and SchemaLessRow implimentations? 
public class Row implements Serializable {

	private Map<String, Column> columns;

	public Row() {
		columns = new HashMap();
	}

	public static class RowVal {
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

		public RowVal(String _name, Column _col) {
			name = _name;
			col = _col;
		}

	}

	public Row(Map<String, Column> cols) {
		columns = cols;
	}

	public Row(Row.RowVal... cols) {
		this();
		for (Row.RowVal entry : cols) {
			this.addColumn(entry.getName(), entry.getCol().getValue());
		}
	}

	public void addColumn(String name, Object col) {
		// System.out.print(name);
		columns.put(name, new Column(col));
	}

	/*
	 * public Map<String, Column> getColumns(){ return columns; }
	 */
	public Collection<Column> getRawColumns() {
		return columns.values();
	}

	Object getColumn(String name) {
		System.out.println("getColumn " + name.toCharArray());
		Column column = columns.get(name);
		System.out.println("\t column = " + column);
		System.out.println("\t cal = " + column.getValue());
		return column.getValue();
	}

	@Override
	public String toString() {
		return this.toString(0);
	}
	public String toString(int offset) {
		final StringBuilder sb = new StringBuilder();
		int size = columns.entrySet().size();
		int i = 0;
		for (Map.Entry<String, Column> entry : columns.entrySet()) {
			i++;
			for(int j=0; j < offset; j++)
		    {
		        sb.append(" ");
		    }
			sb.append(entry.getKey()).append(": ").append(entry.getValue().getValue());
			if (i < size){
				sb.append(",\n");
			}
			
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object ob) {
		if (ob == null)
			return false;
		if (ob.getClass() != getClass())
			return false;
		if (ob == this)
			return true;

		Row other = (Row) ob;
		Map<String, Column> m1 = this.columns;
		Map<String, Column> m2 = other.columns;
		if (m1.size() != m2.size())
			return false;
		for (String key : m1.keySet())
			if (!m1.get(key).equals(m2.get(key)))
				return false;
		return true;
	}

	public Set<Map.Entry<String, Object>> entrySet() {
		Set<Map.Entry<String, Object>> set = new HashSet();
		for (Map.Entry<String, Column> entry : columns.entrySet()) {
			set.add(new AbstractMap.SimpleEntry<String, Object>(entry.getKey(),
					entry.getValue().getValue()));
		}
		return set;
	}

	public int size() {
		return this.columns.size();
	}

	public Struct toStruct(Schema schema) {
		Struct struct = new Struct(schema);
		for (Map.Entry<String, Column> entry : columns.entrySet()) {
			struct.put(entry.getKey(), entry.getValue().getValue());
		}
		return struct;
	}

	static public Row fromStruct(Struct struct) {
		Row row = new Row();
		if (struct != null) {
			for (Field field : struct.schema().fields()) {
				if (struct.get(field) != null){
				   row.addColumn(field.name(), struct.get(field));
				}
			}
		}
		return row;
	}

}
