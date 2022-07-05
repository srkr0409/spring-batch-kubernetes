package com.example.batchprocessing;

public class CSVData {
	
	String[] data;
	String fileName;
	boolean isLast;
	
	
	public CSVData(String[] data, String fileName) {
		super();
		this.data = data;
		this.fileName = fileName;
	}
	
	public String[] getData() {
		return data;
	}
	public void setData(String[] data) {
		this.data = data;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public boolean isLast() {
		return isLast;
	}
	public void setLast(boolean isLast) {
		this.isLast = isLast;
	}
	
	
}
