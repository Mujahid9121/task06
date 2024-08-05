package com.task06;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
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
@EnvironmentVariables({
		@EnvironmentVariable(key = "target_table", value = "${target_table}")
})
public class AuditProducer implements RequestHandler<DynamodbEvent, String> {

	private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
	private final DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
	private final String targetTableName = System.getenv("target_table");
	private final Table auditTable = dynamoDB.getTable(targetTableName);

	@Override
	public String handleRequest(DynamodbEvent event, Context context) {
		for (DynamodbEvent.DynamodbStreamRecord streamRecord : event.getRecords()) {
			String eventType = streamRecord.getEventName();
			switch (eventType) {
				case "INSERT":
					handleInsertEvent(streamRecord.getDynamodb().getNewImage());
					break;
				case "MODIFY":
					handleModifyEvent(streamRecord.getDynamodb().getNewImage(), streamRecord.getDynamodb().getOldImage());
					break;
				default:
					// Ignore other event types
					break;
			}
		}
		return "Processing Complete";
	}

	private void handleModifyEvent(Map<String, AttributeValue> newImage, Map<String, AttributeValue> oldImage) {
		String key = newImage.get("key").getS();
		int previousValue = Integer.parseInt(oldImage.get("value").getN());
		int currentValue = Integer.parseInt(newImage.get("value").getN());

		if (currentValue != previousValue) {
			Item auditEntry = new Item()
					.withPrimaryKey("id", UUID.randomUUID().toString())
					.withString("itemKey", key)
					.withString("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.now().atOffset(ZoneOffset.UTC)))
					.withString("changedAttribute", "value")
					.withInt("previousValue", previousValue)
					.withInt("currentValue", currentValue);
			auditTable.putItem(auditEntry);
		}
	}

	private void handleInsertEvent(Map<String, AttributeValue> newImage) {
		String key = newImage.get("key").getS();
		int value = Integer.parseInt(newImage.get("value").getN());

		Map<String, Object> itemData = new HashMap<>();
		itemData.put("key", key);
		itemData.put("value", value);

		Item auditEntry = new Item()
				.withPrimaryKey("id", UUID.randomUUID().toString())
				.withString("itemKey", key)
				.withString("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.now().atOffset(ZoneOffset.UTC)))
				.withMap("itemData", itemData);

		auditTable.putItem(auditEntry);
	}
}
