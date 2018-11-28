import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
	
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	private String joinType = null;
	private Date date1 = null;
	private Date date2 = null;
	private Date date3 = null;
	private Integer count = 0;
	
	protected void setup(Context context) throws IOException, InterruptedException {

		try {
			
			joinType = context.getConfiguration().get("join.type");
			date1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(context.getConfiguration().get("log1"));
			date2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(context.getConfiguration().get("log2"));
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
				
	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		listA.clear();
		listB.clear();
		
		/* One reducer will get all the values of same key(userid) 
		 * Add the values either name or comments into separate list depending on identifier */
		
		for(Text val : values) {		
			
			if(val.charAt(0) == '!'){
				listA.add(new Text(val.toString().substring(1)));
			}else if (val.charAt(0) == '@'){
				listB.add(new Text(val.toString().substring(1)));
			}
		} 
		
		try {
			performJoins(context);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		/* performs join operation */
	}
	
	
	public void performJoins(Context context) throws IOException, InterruptedException, ParseException {
	
					
		if(joinType.equalsIgnoreCase("inner")){				/* Checks if its inner join */
			if(!listA.isEmpty() && !listB.isEmpty()){		/* if both the lists are not empty */
				for(Text A : listA){						/* iterate through every entry in List A for users */
					for (Text B : listB){					/* Gets the corresponding value from List B(comments) for given userid */
						
						date3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(B.toString());
						if(date1.before(date3) && date2.after(date3)){
							count++;
						}
					}
					context.write(A, new IntWritable(count));				/* Final output key value pair */
				}
			}
		}else{
			throw new RuntimeException("Join Type not set");
		}
	
	}

}
