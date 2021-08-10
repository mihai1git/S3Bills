package com.amazonaws.local.mihai.s3bills.dao;

import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.local.mihai.s3bills.GetBillsVersions;
import com.amazonaws.local.mihai.s3bills.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

public class S3Dao {


	private AmazonS3Client conn_tS3Get_1;
	
	public S3Dao() {
		conn_tS3Get_1 = buildS3Client();
	}
	

	/**
	 * download from AWS S3 objects specified in parameter with bucket, key, version
	 * @param objectsWithVersion notification message with S3 objects identifiers
	 */
	 public void transferBills(List<S3Object> objectsWithVersion) {
		 
		 try {
				TransferManager tm_tS3Get_1 = null;
				try {

					tm_tS3Get_1 = TransferManagerBuilder.standard().withS3Client(conn_tS3Get_1).build();
					
					for (S3Object s3obj : objectsWithVersion) {
						
						GetObjectRequest getObjectRequest_tS3Get_1 = new GetObjectRequest(s3obj.getBucketName(), s3obj.getKey(), s3obj.getVersionId());

						Download download_tS3Get_1 = tm_tS3Get_1.download(
								getObjectRequest_tS3Get_1,
								new java.io.File(GetBillsVersions.fileStorageRoot + "\\" + s3obj.getKeyAsFileVersioned ()),
								null, 0l, true);

						download_tS3Get_1.waitForCompletion();
						
						System.out.println("downoladed file: " + s3obj.getKeyAsFileVersioned ());
					}

				} catch (java.lang.Exception e_tS3Get_1) {

					System.err.println(e_tS3Get_1.getMessage());

				} finally {

					if (tm_tS3Get_1 != null) {
						tm_tS3Get_1.shutdownNow(false);
					}
				}
				
		 } finally {
			 
		 }
	 }
	 
	 /**
	  * uses secrets
	  * @return S3  built client
	  */
	 private AmazonS3Client buildS3Client () {
		 
		 AWSCredentials credentials = null;
		 
		 AWSCredentials credentials_tS3Connection_1 = new BasicAWSCredentials(
					"AKIAIGJELJGD7A64ZTSA", "wEaC9qDqYTdqwUYLKbYONVJsxrafWzxxEUpCCWpb");
			AWSCredentialsProvider credentialsProvider_tS3Connection_1 = new StaticCredentialsProvider(
					credentials_tS3Connection_1);

			ClientConfiguration cc_tS3Connection_1 = new ClientConfiguration();
			cc_tS3Connection_1.setUserAgent("Eclipse Version: 2020-09 (4.17.0) on localhost");

			AmazonS3 conn_tS3Connection_1 = AmazonS3ClientBuilder.standard()
					.withRegion("us-east-2")
					.withCredentials(credentialsProvider_tS3Connection_1)
					.withClientConfiguration(cc_tS3Connection_1).build();

			// This method is just for test connection.
			conn_tS3Connection_1.getS3AccountOwner();
			
			AmazonS3Client conn_tS3Get_1 = (AmazonS3Client) conn_tS3Connection_1;
			
			return conn_tS3Get_1;
	 }
}
