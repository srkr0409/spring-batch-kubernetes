package com.example.batchprocessing;

public class PersonData {
	
	Person person;
	String fileName;
	boolean isLast;
	
	
	public PersonData(Person person, String fileName, boolean isLast) {
		super();
		this.person = person;
		this.fileName = fileName;
		this.isLast = isLast;
	}
	
	public Person getPerson() {
		return person;
	}
	public void setPerson(Person person) {
		this.person = person;
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
