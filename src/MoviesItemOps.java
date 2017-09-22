import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;


public class MoviesItemOps {

	public static void main(String[] args) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		
		DynamoDB dynamoDB = new DynamoDB(client);
		
		Table table = dynamoDB.getTable("Movies");
		
		putItemSample(table);
		getItemSample(table);
		updateItemSample(table);
		incrementAtomicCounterSample(table);
	}
	
	public static void putItemSample(Table table) {
		int year = 2015;
		String title = "The Big New Moive 1";
		
		Map<String, Object> infoMap = new HashMap<String, Object>();
		infoMap.put("plot", "Nothing happens at all");
		infoMap.put("rating", 0);
		
		try {
			System.out.println("Adding happens at all.");
			PutItemOutcome outcome = table.putItem(new Item().withPrimaryKey("year", year, "title", title).withMap("info", infoMap));
		
			System.out.println("PutItem succeeded: \n" + outcome.toString());
		} catch (Exception e) {
			System.out.println("Unable to add Item. ErrMsg: " + e.getMessage());
		}
		
	}
	
	public static void getItemSample(Table table) {
		int year = 2015;
		String title = "The Big New Moive";
		
		GetItemSpec spec = new GetItemSpec().withPrimaryKey("year", year, "title", title);
		try {
			System.out.println("Attempt to read the item... ");
			Item outcome = table.getItem(spec);
			System.out.println("GetItem succeeded:" + outcome);
		} catch (Exception e) {
			System.out.println("Unable to read item: " + year + " " + title);
			System.out.println("ErrorMessage: " + e.getMessage());
		}
	}
	
	public static void updateItemSample(Table table) {
		int year = 2015;
		String title = "The Big New Moive";
		
		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("year", year, "title", title)
				.withUpdateExpression("set info.rating = :r, info.plot1 = :p, info.actors = :a")
				.withValueMap(new ValueMap().withNumber(":r", 5.5)
						.withString(":p", "Everything happens all at once.")
						.withList(":a", Arrays.asList("Larry", "Moe", "Curly")))
				.withReturnValues(ReturnValue.UPDATED_NEW);
		
		try {
			System.out.println("Updating the item ...");
			UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
			System.out.println("UpdateItem succeeded: " + outcome);
		} catch (Exception e) {
			System.out.println("Unable to update item. ErrMsg: " + e.getMessage());
		}
	}
	
	public static void incrementAtomicCounterSample(Table table) {
		int year = 2015;
		String title = "The Big New Moive";
		
		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("year", year, "title", title)
				.withUpdateExpression("set info.rating = info.rating + :val, info1 = :val")
				.withValueMap(new ValueMap().withNumber(":val", 1))
				.withReturnValues(ReturnValue.ALL_NEW);
		
		try {
			System.out.println("Updating the item ...");
			UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
			System.out.println("UpdateItem succeeded: " + outcome.getItem().toJSONPretty());
		} catch (Exception e) {
			System.out.println("Unable to update item. ErrMsg: " + e.getMessage());
		}
	}

}
