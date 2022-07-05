package com.example.batchprocessing;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;


public class PersonItemReader implements ItemReader<CSVData> {

	private static String buckeName = "aws-devops-course-rama";
	private  S3Client s3;
	private List<String[]> rows;
	List<String> objectList;
	private int objectListIndex;
	private int nextIndex;
	private String currentKey;
	
	

	public PersonItemReader(S3Client s3) {
		super();
		this.s3 = s3;
	}

	@Override
	public CSVData read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		
		String[] nextRow = null;
		CSVData data = null;
		
		if (CollectionUtils.isNotEmpty(objectList)) {
			
			 if (nextIndex < rows.size()) {
		        	nextRow = rows.get(nextIndex);
		        	data = new CSVData(nextRow, objectList.get(objectListIndex-1));
		        	nextIndex++;
		        	
		        	if (nextIndex == rows.size()) {
		        		data.setLast(true);
		        		 if (objectListIndex < objectList.size()) {
		        			 rows = null;
		            		 readFromS3(s3, buckeName, objectList.get(objectListIndex));
		            		 nextIndex = 0;
		            		 objectListIndex++;
		            	 }
		        	}else {
		        		data.setLast(false);
		        	}
		        }
		        else {
		        	nextIndex = 0;
		        }
		}
	        return data;
	}

	@BeforeStep
	public void readObjects(){
		nextIndex = 0;
    	 objectList = listBucketObjects(s3, buckeName);
    	 if (CollectionUtils.isNotEmpty(objectList)) {
    		 readFromS3(s3, buckeName, objectList.get(objectListIndex));
    		 objectListIndex++;
    	 }
     }
	
	public void readFromS3(S3Client s3, String bucketName, String key){
		
    	 GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                 .bucket(bucketName)
                 .key(key)
                 .build();
    	 
		  ResponseInputStream<GetObjectResponse> s3object = s3.getObject(getObjectRequest); 
		  CSVReader csvReader = new CSVReader(new InputStreamReader(s3object)); 
		   try { 
			   rows = csvReader.readAll();
			   } catch (IOException |CsvException e1) {
						   // TODO Auto-generated catch block e1.printStackTrace(); 
			    } 
		   }
		   
	
    public List<String> listBucketObjects(S3Client s3, String bucketName ) {
    	objectList = new ArrayList();
        try {
             ListObjectsRequest listObjects = ListObjectsRequest
                     .builder()
                     .bucket(bucketName)
                     .build();

             ListObjectsResponse res = s3.listObjects(listObjects);
             List<S3Object> objects = res.contents();

             for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                 S3Object myValue = (S3Object) iterVals.next();
                 System.out.print("\n The name of the key is " + myValue.key());
                 objectList.add(myValue.key());
              }

         } catch (S3Exception e) {
             System.err.println(e.awsErrorDetails().errorMessage());
             System.exit(1);
         }
        return objectList;
     }
}
