import java.io.File;
import java.util.Date;
import java.util.Iterator;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class MoviesLoadData {
	public static void main(String[] args) throws Exception {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
		
		DynamoDB dynamoDB = new DynamoDB(client);
		
		Table table = dynamoDB.getTable("Movies");
		JsonParser parser = new JsonFactory().createJsonParser(new File("data/moviedata.json"));
		
		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();
		
		ObjectNode currentNode;
		int i = 0;
		
		while (iter.hasNext()) {
			currentNode = (ObjectNode) iter.next();
			
			int year = currentNode.path("year").asInt();
			String title = currentNode.path("title").asText();
			
			try {
				table.putItem(new Item().withPrimaryKey("year", year, "title", title)
						.withJSON("info", currentNode.path("info").toString()));
				
				if (i % 100 == 0) {
					System.out.println("Current count: " + i + ", time:" + new Date());
				}
			} catch (Exception e) {
				System.err.println("Unable to add moive: " + year + " " + title);
				System.err.println(e.getMessage());
				break;
			}
			i++;
		}
		
		parser.close();
	}
}
