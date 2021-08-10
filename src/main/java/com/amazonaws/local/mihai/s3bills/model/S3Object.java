package com.amazonaws.local.mihai.s3bills.model;

public class S3Object {

	private String bucketName;
	
	private String key;
	
	private String versionId;
	
	public S3Object () {
		
	}
	
	public S3Object (String bucketName, String key, String versionId) {
		this.bucketName = bucketName;
		this.key = key;
		this.versionId = versionId;
	}
	
	public String toString () {
		return bucketName + "/" + key + "_" + versionId;
	}
	
	public String getKeyAsFileVersioned () {
		return key.substring(0, key.lastIndexOf('.')) + "_" + versionId + key.substring(key.lastIndexOf('.'));
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getVersionId() {
		return versionId;
	}

	public void setVersionId(String versionId) {
		this.versionId = versionId;
	}
	
	
}
