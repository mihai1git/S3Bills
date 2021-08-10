package com.amazonaws.local.mihai.s3bills;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.local.mihai.s3bills.dao.DiskDao;
import com.amazonaws.local.mihai.s3bills.dao.S3Dao;
import com.amazonaws.local.mihai.s3bills.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;


public class GetBillsVersions {
	
	public static final String fileStorageRoot = "C:\\MyDocuments\\amazon\\bill\\all";
	public static final String s3ObjectBucket = "billing-reports-mihaiadam";
	public static final String jsonBillsVersions = fileStorageRoot + "\\filesVersioned_2021.08.05.json";
	
	private S3Dao s3dao;
	private DiskDao diskdao;
	
	
	 public static void main(String[] args) throws IOException {
		 
		 
	 
		 List<S3Object> objectsWithVersion = new ArrayList<S3Object>();
		 
		 objectsWithVersion.add(new S3Object("billing-reports-mihaiadam", "255864408802-aws-billing-csv-2021-08.csv", "OI26.9ErM03XuWrVkuDaypgTNG6u7X4g"));
		 objectsWithVersion.add(new S3Object("billing-reports-mihaiadam", "255864408802-aws-billing-csv-2021-08.csv", "xmPmvP0xYsPq7bnxMbJMrPiM1JC.YZuh"));
		 objectsWithVersion.add(new S3Object("billing-reports-mihaiadam", "255864408802-aws-billing-csv-2021-08.csv", "4GZeV5Jftvz6SV1scek3Pid.3QJnRAL9"));
		 
		 
		 GetBillsVersions billsSrv = new GetBillsVersions();
		 
		 objectsWithVersion = billsSrv.getDiskdao().loadBillsNotification(GetBillsVersions.jsonBillsVersions);
		 
		 //System.out.println(Arrays.toString(objectsWithVersion.toArray()));
		 System.out.println("objectsWithVersion.size: " + objectsWithVersion.size());
		 System.out.println("objectsWithVersion.15 " + objectsWithVersion.get(15));
		 System.out.println("objectsWithVersion.getKeyAsFileVersioned: " + objectsWithVersion.get(15).getKeyAsFileVersioned());
		 
		 List<S3Object> newObjectsWithVersion = billsSrv.getDiskdao().getNewFiles(objectsWithVersion);
		 
		 System.out.println("newObjectsWithVersion.size: " + newObjectsWithVersion.size());
		  
		 billsSrv.getS3dao().transferBills(newObjectsWithVersion);
	 }


		public S3Dao getS3dao() {
			return s3dao;
		}

		public void setS3dao(S3Dao s3dao) {
			this.s3dao = s3dao;
		}

		public DiskDao getDiskdao() {
			return diskdao;
		}

		public void setDiskdao(DiskDao diskdao) {
			this.diskdao = diskdao;
		}
	 

	 
}
