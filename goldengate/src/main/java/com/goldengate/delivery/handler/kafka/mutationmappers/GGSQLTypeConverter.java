package com.goldengate.delivery.handler.kafka.mutationmappers;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.rogers.cdc.api.schema.AbstractSQLTypeConverter;
import com.goldengate.atg.datasource.DsColumn;

public class GGSQLTypeConverter implements AbstractSQLTypeConverter<DsColumn> {
    // TODO: Default time zone?
	@Override
	public boolean getBoolean(DsColumn col) throws SQLException{
		String val =  getStringInt(col);
		if  (val == "t")
			return true;
		else if(val == "f"){
			return false;
		}
		else{
			throw new SQLException("boolean column must be either 't' or 'f', got invalid value:" + val);
		}
	}

	@Override
	public byte[] getBytes(DsColumn col) throws SQLException{
		// TODO Somehow check encoding? 
		// Do we care to trim?
		String val =  getStringInt(col);
		return val.getBytes() ;
	}

	@Override
	public String getString(DsColumn col) throws SQLException{
		String val =  getStringInt(col);
		return val;
	}

	@Override
	public String getNString(DsColumn col) throws SQLException {
		// TODO Auto-generated method stub
		String val =  getStringInt(col);
		throw new SQLException("NString column not supported ");
	}

	@Override
	public Short getShort(DsColumn col) throws SQLException {
		String val =  getStringInt(col);
		return Short.parseShort(val);
	}

	@Override
	public Integer getInt(DsColumn col) throws SQLException{
		String val =  getStringInt(col);
		return Integer.parseInt(val);
	}

	@Override
	public Long getLong(DsColumn col) throws SQLException{

		String val =  getStringInt(col);
		return Long.parseLong(val);
	}

	@Override
	public Float getFloat(DsColumn col) throws SQLException {
		String val =  getStringInt(col);
		return Float.parseFloat(val);
	}

	@Override
	public Double getDouble(DsColumn col) throws SQLException {
		// TODO:  How does oracle handle strinct floating point precision?
		String val =  getStringInt(col);
		return Double.parseDouble(val);
	}

	//@Override
	/*public BigDecimal getBigDecimal(DsColumn col, int scale) throws SQLException {
		// TODO Is there more logic to it? Corner cases?
		// Can we do this faster? 

		String val =  getStringInt(col);
		BigDecimal res = new  BigDecimal(val);
		res.setScale(scale);
		return res;
	}*/
	@Override
	public BigDecimal getBigDecimal(DsColumn col) throws SQLException {
		// TODO What about scale? Make sure that GG passes a proper string representation of a BigDecimal 

		String val =  getStringInt(col);
		BigDecimal res = new  BigDecimal(val);
		
		return res;
	}

	@Override
	public Time getTime(DsColumn col, Calendar cal) throws SQLException {

		String val =  getStringInt(col);
		DateTime dt = new DateTime(DateTimeZone.UTC);
		//Time res = new Time(cal.getTime().getTime());
		DateTimeFormatter sdf =  ISODateTimeFormat.time();
		return new Time(sdf.parseDateTime(val).toDateTime(DateTimeZone.UTC).getMillis());
	}

	@Override
	public Timestamp getTimestamp(DsColumn col, Calendar cal) throws SQLException{
		// For TIMESTAMP WITH TIME ZONE in Oracle, 3 options
		// 1) Default GG Extract aborts 
		// 2) INCLUDEREGIONID is set format is YYYY-MM-DD HH:MI.SS.FFFFFF
		// 3) INCLUDEREGIONIDWITHOFFSET is set  the format is YYYY-MM-DD HH:MI.SS.FFFFFF TZH:TZM (aka - time zone code is added) 
		// We intend to support options #2 only....yet, the date we get seems to be in ISO 8601 format anyway. So I guess we can support both 2 and 3
		String val =  getStringInt(col);
		DateTimeFormatter sdf =  ISODateTimeFormat.dateTime();

		return new Timestamp(sdf.parseDateTime(val).toDateTime(DateTimeZone.UTC).getMillis());
	}

	@Override
	public Date getDate(DsColumn col, Calendar cal) throws SQLException{
		// TODO Auto-generated method stub
		//String val =  getStringInt(col);
		String val =  getStringInt(col);
		DateTimeFormatter sdf =  ISODateTimeFormat.date();

		return new Date(sdf.parseDateTime(val).toDateTime(DateTimeZone.UTC).getMillis());
	}

	@Override
	public String getClob(DsColumn col) throws SQLException{
		// TODO Auto-generated method stub
		
		String val =  getStringInt(col);
		return val;
	}

	@Override
	public String getNClob(DsColumn col) throws SQLException{
		String val =  getStringInt(col);
		throw new SQLException("NClob column not supported ");
	}

	@Override
	public byte[] getBlob(DsColumn col) throws SQLException{
		// TODO Auto-generated method stub
		this.getBytes(col);
		return null;
	}

	@Override
	public String getURL(DsColumn col) throws SQLException{
		// TODO Do some extra checks here?
		String val =  getStringInt(col);
		return val;
	}

	@Override
	public String getSQLXML(DsColumn col) throws SQLException{
		// TODO Some extra checks
		String val =  getStringInt(col);
		return val;
	}

	@Override
	public boolean isNull(DsColumn col) throws SQLException{
		return col.isValueNull();
	}
	
	private String getStringInt (DsColumn col) throws SQLException{
		String val = col.getAfterValue();
		if (val == null){
			throw new SQLException("column val should not be null");
		}
		return val;
	}
	

}
