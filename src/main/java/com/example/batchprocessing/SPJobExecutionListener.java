package com.example.batchprocessing;


import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

@Component
public class SPJobExecutionListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
    	System.out.println("Called beforeJob()................");
    	
    	///List<String[]> csvData = createCsvDataSimple();

        // default all fields are enclosed in double quotes
        // default separator is a comma
		/*
		 * try (CSVWriter writer = new CSVWriter(new FileWriter("/cache/test.csv"))) {
		 * // writer.writeAll(csvData); } catch (IOException e) { // TODO Auto-generated
		 * catch block e.printStackTrace(); }
		 */
    	
    	String destFileName = "/cache/s3file.csv";
    	//String destFileName = "c:\\test\\s3file.csv";
    	
    	try {
			Files.deleteIfExists(Paths.get(destFileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    	 Region region = Region.US_EAST_2;
    	 String access_key_id = "AKIAVW6LG7TJDDJDXGWH";
    	 String secret_access_key = "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";
    	 
    	 AwsBasicCredentials awsCreds = AwsBasicCredentials.create(access_key_id, secret_access_key);

    	 S3Client s3 = S3Client.builder()
    			        .region(region)
    			        .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
    			        .build();
    	 
    	 System.out.println("LIST BUCKET..... : " + s3.listBuckets());
    	 
    	 GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                 .bucket("aws-devops-course-rama")
                 .key("sample-data.csv")
                 .build();
    	 
			  Path s3Path = Paths.get(destFileName); 
			  GetObjectResponse getObjectResponse = s3.getObject(getObjectRequest, s3Path);
			  System.out.println("LIST OBJECT..... : " + s3.getObject(getObjectRequest).toString());
			 
    	 
			
				/*
				 * byte[] bytes;
				 * 
				 * ResponseInputStream<GetObjectResponse> s3object =
				 * s3.getObject(getObjectRequest); CSVReader csvReader = new CSVReader(new
				 * InputStreamReader(s3object)); List<String[]> rows; try { rows =
				 * csvReader.readAll(); System.out.println(rows); } catch (IOException |
				 * CsvException e1) { // TODO Auto-generated catch block e1.printStackTrace(); }
				 */			 
		
    	 
			/*
			 * // READ FROM CLASS PATH SPJobExecutionListener app = new
			 * SPJobExecutionListener();
			 * 
			 * //String fileName = "database.properties"; String fileName =
			 * "sample-data.csv";
			 * 
			 * System.out.println("getResourceAsStream : " + fileName); InputStream is =
			 * app.getFileFromResourceAsStream(fileName); try { //String destFileName =
			 * "/cache/test.csv"; String destFileName = "c:\\test\\monitor.csv"; byte[]
			 * strToBytes = is.readAllBytes(); Path path = Paths.get(destFileName);
			 * Files.write(path, strToBytes); } catch (IOException e) { // TODO
			 * Auto-generated catch block e.printStackTrace(); } printInputStream(is);
			 */

    }
    
    // print input stream
    private static void printInputStream(InputStream is) {

        try (InputStreamReader streamReader =
                    new InputStreamReader(is, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void afterJob(JobExecution jobExecution) {
        System.out.println("Called afterJob().................");
    }
    
    
    
 // get a file from the resources folder
    // works everywhere, IDEA, unit test and JAR file.
    private InputStream getFileFromResourceAsStream(String fileName) {

        // The class loader that loaded the class
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);

        // the stream holding the file content
        if (inputStream == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return inputStream;
        }

    }
    
    
    private static List<String[]> createCsvDataSimple() {
        String[] header = {"id", "name", "address", "phone"};
        String[] record1 = {"1", "first name", "address 1", "11111AAA"};
        String[] record2 = {"2", "second name", "address 2", "22222BBBB"};

        List<String[]> list = new ArrayList<>();
        list.add(header);
        list.add(record1);
        list.add(record2);

        return list;
    }
}