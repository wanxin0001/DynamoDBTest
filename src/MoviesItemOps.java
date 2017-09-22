import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;


public class MoviesItemOps {

	public static void main(String[] args) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		
		DynamoDB dynamoDB = new DynamoDB(client);
		
		Table table = dynamoDB.getTable("Movies");
		
		putItemSample(table);
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

}
