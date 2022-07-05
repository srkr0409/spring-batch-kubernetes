package com.example.batchprocessing;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.slf4j.Logger;


public class SPItemWriteListener implements ItemWriteListener < PersonData > {

    Logger logger = LoggerFactory.getLogger(SPItemWriteListener.class);
   
    S3Client s3Client;
    
    
	public SPItemWriteListener(S3Client s3Client) {
		super();
		this.s3Client = s3Client;
	}

	@Override
	public void beforeWrite(List<? extends PersonData> items) {
		 logger.info("beforeWrite");
		
	}

	@Override
	public void afterWrite(List<? extends PersonData> items) {
		 logger.info("afterWrite");
		 
		 //aws-devops-use1
		 for (PersonData personData : items) {
			 if (personData.isLast) {
				 
				 String copyObjectResult = copyBucketObject(s3Client, "aws-devops-course-rama", personData.getFileName(), "cicd-rama-build");
				 logger.info("COPY OBJECT RESULT " + copyObjectResult);
				 logger.info("DELETING FILE FROM BUCKET " + personData.getFileName());
				 deleteBucketObject(s3Client, "aws-devops-course-rama", personData.getFileName());
			 }
			 
		 }
		
	}

	@Override
	public void onWriteError(Exception exception, List<? extends PersonData> items) {
		logger.info("onWriteError");
		
	}
	
	
	 public static  void deleteBucketObject (S3Client s3, String bucketName, String objectKey) {
		 DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
	                .bucket(bucketName)
	                .key(objectKey)
	                .build();

		 s3.deleteObject(deleteObjectRequest);
		 
	 }
	
    public static String copyBucketObject (S3Client s3, String fromBucket, String objectKey, String toBucket) {

        String encodedUrl = null;
        try {
            encodedUrl = URLEncoder.encode(fromBucket + "/" + objectKey, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            System.out.println("URL could not be encoded: " + e.getMessage());
        }
        String destinationKey = getDatePreFix() + "/" + objectKey;
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .copySource(encodedUrl)
                .destinationBucket(toBucket)
                .destinationKey(destinationKey)
                .build();

        try {
            CopyObjectResponse copyRes = s3.copyObject(copyReq);
            return copyRes.copyObjectResult().toString();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }
    
    private static String getDatePreFix() {
    	Date date = new Date();  
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");  
	    String strDate = formatter.format(date);
	    return strDate;
    }
}
