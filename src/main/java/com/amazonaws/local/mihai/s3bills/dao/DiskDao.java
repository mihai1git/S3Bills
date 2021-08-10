package com.amazonaws.local.mihai.s3bills.dao;

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

import com.amazonaws.local.mihai.s3bills.GetBillsVersions;
import com.amazonaws.local.mihai.s3bills.model.S3Object;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DiskDao {

	
	 
	 public List<S3Object> getNewFiles (List<S3Object> allFiles) {
		 List<S3Object> newFiles = new ArrayList<S3Object>();
		 
		 List<String> localFiles = getLocalBills();
		 
		 for (S3Object obj : allFiles) {
			 if (localFiles.contains(obj.getKeyAsFileVersioned())) 
				 System.out.println("local found: " + obj.getKeyAsFileVersioned());
			 else 
				 newFiles.add(obj);
		 }
		 
		 
		 return newFiles;
	 }
	 
	 private List<String> getLocalBills() {
		 
		 List<String> filesInFolder = new ArrayList<String>();
		 
		 try {
			 List<Path> pathInFolder = Files.walk(Paths.get(GetBillsVersions.fileStorageRoot))
		     .filter(Files::isRegularFile)
		     .filter(p -> p.getFileName().toString().endsWith("csv"))
		     .filter(p -> p.getFileName().toString().contains("-aws-billing-"))
		     .collect(Collectors.toList());
			 
			 for (Path file : pathInFolder) {
				 filesInFolder.add(file.getFileName().toString());
			 }
			 
		 } catch(IOException ex) {
			 throw new RuntimeException(ex);
		 }
		 


		 return filesInFolder;
	 }
	 
	 
	 public List<S3Object> loadBillsNotification (String jsonBillsPath) {
		 
		 List<S3Object> result = null;
		
		if (doesObjectExist(jsonBillsPath)) {
			System.out.println("Bills versions file found");
		} else {
			throw new RuntimeException("File " + jsonBillsPath + " does not exists !");
		}
		
		String filePathString = jsonBillsPath;
			
		try {
	
			Path path = Paths.get(filePathString);
			InputStream inputStream = Files.newInputStream(path);
			String jsonObject = readFromInputStream(inputStream);
			
	    	System.out.println("jsonObject as string length: " + jsonObject.length());
		    	
	    	ObjectMapper om = new ObjectMapper().setSerializationInclusion(Include.NON_NULL);
	    	
	    	om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	    	om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
	    	om.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
	    	
	    	try {
	    		result = om.readValue(jsonObject,  new TypeReference<List<S3Object>>() { });
	    		
	    	} catch (Exception ex) {
	    		ex.printStackTrace();
	    		throw new RuntimeException(ex);
	    	}
	    	
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		
		//assertThat(result.get(0), instanceOf(S3Object.class));

		return result;
	 }
	 
	private String readFromInputStream(InputStream inputStream) throws IOException {
		
	    StringBuilder resultStringBuilder = new StringBuilder();
	    try (BufferedReader br
	      = new BufferedReader(new InputStreamReader(inputStream))) {
	        String line;
	        while ((line = br.readLine()) != null) {
	            resultStringBuilder.append(line).append("\n");
	        }
	    }
	    
	  return resultStringBuilder.toString();
	}
 
	private boolean doesObjectExist (String filePathString) {
		
		System.out.println("filePathString: " + filePathString);
		
		Path path = Paths.get(filePathString);
		
		return Files.exists(path);
	}
}
