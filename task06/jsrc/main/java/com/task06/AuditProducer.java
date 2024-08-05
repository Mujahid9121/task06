package com.task06;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		isPublishVersion = false,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DynamoDbTriggerEventSource(
		targetTable = "Configuration",
		batchSize = 1
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "target_table", value = "${target_table}")
})
public class AuditProducer implements RequestHandler<DynamodbEvent, String> {
	private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	private final DynamoDB dynamoDb = new DynamoDB(client);
	private final String DYNAMODB_TABLE_NAME = System.getenv("target_table");
	private final Table audittable = dynamoDb.getTable(DYNAMODB_TABLE_NAME);
	@Override
	public String handleRequest(DynamodbEvent event, Context context) {

		for(DynamodbEvent.DynamodbStreamRecord record: event.getRecords()){
			String eventName=record.getEventName();
			if(eventName.equals("INSERT")){
				addDataToAuditTable(record.getDynamodb().getNewImage());
				break;
			}
			else if(eventName.equals("MODIFY")){
				modifyDataToAuditTable(record.getDynamodb().getNewImage(),record.getDynamodb().getOldImage());
			}
			else{
				break;
			}
		}
		return "";
	}

	private void modifyDataToAuditTable(Map<String, AttributeValue> newImage, Map<String, AttributeValue> oldImage) {
		String key = newImage.get("key").getS();
		int previousValue = Integer.parseInt(oldImage.get("value").getN());
		int newValue = Integer.parseInt(newImage.get("value").getN());

		if(newValue!=previousValue){
			Item updateAuditItem= new Item()
					.withPrimaryKey("id",UUID.randomUUID().toString())
					.withString("itemKey",key)
					.withString("modificationTime",DateTimeFormatter.ISO_INSTANT
							.format(Instant.now().atOffset(ZoneOffset.UTC)))
					.withString("updatedAttribute","value")
					.withInt("oldValue",previousValue)
					.withInt("newValue",newValue);
			audittable.putItem(updateAuditItem);
		}
	}

	private void addDataToAuditTable(Map<String, AttributeValue> newImage) {

		String key=newImage.get("key").getS();
		int value=Integer.parseInt(newImage.get("value").getN());

		Map<String,Object> newValue=new HashMap<>();
		newValue.put("key",key);
		newValue.put("value", value);

		Item auditItem=new Item()
				.withPrimaryKey("id",UUID.randomUUID().toString())
				.withString("itemKey",key)
				.withString("modificationTime",DateTimeFormatter.ISO_INSTANT
						.format(Instant.now().atOffset(ZoneOffset.UTC)))
				.withMap("newValue",newValue);

		audittable.putItem(auditItem);
	}
}
