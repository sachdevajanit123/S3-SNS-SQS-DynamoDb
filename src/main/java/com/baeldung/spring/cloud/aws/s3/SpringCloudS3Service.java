package com.baeldung.spring.cloud.aws.s3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

@Component
public class SpringCloudS3Service {

	private static final Logger logger = LoggerFactory.getLogger(SpringCloudS3Service.class);

	@Autowired
	AmazonS3 amazonS3;

	public void uploadObjectS3(String bucketName, String objectName) {

		String destBucketName = createDestinationBucket();
		logger.info("uploading file in s3 bucket" + destBucketName);
		amazonS3.putObject(
				destBucketName, 
				objectName +"_new", 
				new File("src/main/resources/"+objectName+".csv")
				);
	}
	
	public String createDestinationBucket() {

		String bucketName = "dest-invoice-project2";

		try {

			if (!amazonS3.doesBucketExistV2(bucketName)) {
				// Because the CreateBucketRequest object doesn't specify a region, the
				// bucket is created in the region specified in the client.
				amazonS3.createBucket(new CreateBucketRequest(bucketName));

				// Verify that the bucket was created by retrieving it and checking its location.
				String bucketLocation = amazonS3.getBucketLocation(new GetBucketLocationRequest(bucketName));
			}
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process 
			// it and returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
		return bucketName;
	}
	
	

	public void downloadConvertAndUpload(String bucketName, String objectName) throws IOException {
		S3Object s3object = amazonS3.getObject(bucketName, objectName);
		S3ObjectInputStream inputStreamText = s3object.getObjectContent();
		S3ObjectInputStream inputStreamCsv = s3object.getObjectContent();
		File targetFileCsv = new File("src/main/resources/"  +objectName+ ".csv");
		File targetFileText = new File("src/main/resources/" +objectName+ ".text");


		java.nio.file.Files.copy(
				inputStreamText, 
				targetFileText.toPath(), 
				StandardCopyOption.REPLACE_EXISTING);
		/*java.nio.file.Files.copy(
				inputStreamCsv, 
				targetFileCsv.toPath(), 
				StandardCopyOption.REPLACE_EXISTING);*/

		IOUtils.closeQuietly(inputStreamText);
		IOUtils.closeQuietly(inputStreamCsv);


		Map<String, String> valueMap = convertToCSV(objectName);
		uploadObjectS3(bucketName, objectName);
		uploadtoDyanamoDb(valueMap);
	}

	public void uploadtoDyanamoDb(Map<String, String> valueMap) throws IOException{

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
		DynamoDB dynamoDB = new DynamoDB(client);

		String tableName = "Invoice";

		logger.info("Getting Table from DynamoDb" + tableName);
		Table table = dynamoDB.getTable(tableName);

		try {

			Item item = new Item().withPrimaryKey("Customer-ID", valueMap.get("Customer-ID")).withString("Inv-ID", valueMap.get("Inv-ID")).withString("Dated", valueMap.get("Dated"))
					.withString("From", valueMap.get("From")).withString("To", valueMap.get("To")).withString("Amount", valueMap.get("Amount"))
					.withString("SGST", valueMap.get("SGST")).withString("Total", valueMap.get("Total")).withString("InWords", valueMap.get("InWords"));
					
			logger.info("Putting Table data in table" + tableName);
			table.putItem(item);

		}
		catch (Exception e) {
			System.err.println("Create items failed.");
			System.err.println(e.getMessage());

		}
	}


	private Map<String, String> convertToCSV(String objectName) throws IOException{

		Map<String, String> map = new HashMap<>();
		File file = new File("src/main/resources/"+objectName+".text");
		String line=null;
		StringBuffer str = new StringBuffer();  
		BufferedReader br = null;  

		String csvFile = "src/main/resources/"+objectName+".csv";
		FileWriter writer = new FileWriter(csvFile);

		//for header
		CSVUtils.writeLine(writer, Arrays.asList("Customer-ID", "Inv-ID", "Dated", "From", "To", "Amount", "SGST", "Total", "InWords"));
		List<String> list = new ArrayList<>();
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String text=null;
			Set<String> set=new LinkedHashSet<String>();
			try {
				while ((line = reader.readLine()) != null && !line.contains("Items"))
				{

					if(! line.contains("***") && ! line.contains("Invoice") && !line.contains("   "))
					{
						for(String parts:line.split(":"))
						{
							String columnFile=StringUtils.substringBefore(line, ":");
							String dataValue=StringUtils.substringAfter(line, ":");
							set.add(dataValue);
							if(!map.containsKey(columnFile)){
								map.put(columnFile, dataValue);
							}
						}                   
					}

				}
				for (String t : set) {
					list.add(t); 
				}

				CSVUtils.writeLine(writer, list);

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}catch(IOException e)
		{
			e.printStackTrace();
		}
		writer.flush();
		writer.close();
		return map;
	}
	
	
	
}
