package com.baeldung.spring.cloud.aws.sns;



import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.baeldung.spring.cloud.aws.s3.SpringCloudS3Service;

@Component
@Lazy
public class SpringCloudSQS {

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudSQS.class);

    static final String QUEUE_NAME = "invoice-queue";
    
	@Autowired
	SpringCloudS3Service springCloudS3Service;

    
    @Autowired
    QueueMessagingTemplate queueMessagingTemplate;

    @SqsListener(QUEUE_NAME)
    public void receiveMessage(String message, @Header("SenderId") String senderId) throws JSONException  {
        logger.info("Received message: {}, having SenderId: {}", message, senderId);
        JSONObject jsonObject = new JSONObject(message);
        String messageJson = jsonObject.getString("Message");
        JSONObject jsonObjectMessage = new JSONObject(messageJson);
        logger.info("Received messageafter parsing : " + jsonObjectMessage);
        
        JSONArray jsonArray = jsonObjectMessage.getJSONArray("Records");  //"results" JSONArray
        JSONObject zero = jsonArray.getJSONObject(0);  //first JSONObject inside "results" JSONArray
        JSONObject s3 = zero.getJSONObject("s3");
        JSONObject bucket = s3.getJSONObject("bucket");
        String bucketName= bucket.getString("name");
        logger.info("The bucket name is " +bucketName);
        
        JSONObject object = s3.getJSONObject("object");
        String fileName= object.getString("key");	
        logger.info("The file name is " +fileName);
        
        try {
			springCloudS3Service.downloadConvertAndUpload(bucketName, fileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.info("Download unsuccessful");
		}
    }

}