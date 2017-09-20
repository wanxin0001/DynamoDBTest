import java.util.Arrays;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;


public class MoviesCreateTable {
	
	public static void main(String[] args) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder
				.standard()
				.withRegion(Regions.US_WEST_2)
				.build();
		
		DynamoDB dynamoDB = new DynamoDB(client);
		
		String tableName = "Movies";
		
		try {
			System.out.println("Attempting to create table; please wait...");
			Table table = dynamoDB.createTable(tableName, 
					Arrays.asList(new KeySchemaElement("year", KeyType.HASH), new KeySchemaElement("title", KeyType.RANGE)),
					Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N), new AttributeDefinition("title", ScalarAttributeType.S)),
					new ProvisionedThroughput(10L, 10L));
			
			table.waitForActive();
			
			System.out.println("Success. Table status: " + table.toString());
		} catch (Exception e) {
			System.out.println("Unable to create table: ");
			System.out.println(e.getMessage());
		}
	}
}
