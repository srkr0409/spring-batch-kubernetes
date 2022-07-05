package com.example.batchprocessing;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {

	public static void main(String[] args) {
		//dd-M-yyyy
		Date date = new Date();  
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");  
	    String strDate = formatter.format(date);  
	    System.out.println("Date Format with dd-M-yyyy : "+strDate);  
	}
}
