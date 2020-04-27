import org.json.simple.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.FileSystem;

public class ClickRate {

public static void main(String[] args) throws Exception {
	if (args.length < 3) {
		System.err.println("Error: Wrong number of parameters");
		System.err.println("Expected: [clicks] [impressions] [out]");
		System.exit(1);
	}
	
	Path temp_path = new Path("temp");

	Configuration conf1 = new Configuration();
			 
	Job job1 = Job.getInstance(conf1, "impression merge job");
	job1.setJarByClass(ClickRate.class);

	job1.setReducerClass(ClickRate.ImpressionClickedReducer.class);
	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(Text.class);

	MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, ClickMapper.class);
	MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, ImpressionMapper.class);
	FileOutputFormat.setOutputPath(job1, temp_path);

	job1.waitForCompletion(true);

	Configuration conf2 = new Configuration();
	Job job2 = Job.getInstance(conf2, "click rate job");
	
	job2.setJarByClass(ClickRate.class);
	job2.setMapperClass(ClickRate.IdentityMapper.class);
	job2.setReducerClass(ClickRate.SumReducer.class);
	job2.setMapOutputKeyClass(Text.class);
	job2.setMapOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job2, temp_path);
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));


	job2.waitForCompletion(true);
}

/**
 * map: (LongWritable, Text) --> (LongWritable, Text)
 * NOTE: Keys must implement WritableComparable, values must implement Writable
 */
public static class IdentityMapper extends Mapper < LongWritable, Text, 
                                                    Text, Text > {

	@Override
	public void map(LongWritable key, Text val, Context context)
		throws IOException, InterruptedException {
		String entry = val.toString();
		int index = entry.indexOf("	");
		String new_key = entry.substring(0, index);
		String value = entry.substring(index+1);
		context.write(new Text(new_key), new Text(value));
	}

}

/**
 * reduce: (LongWritable, Text) --> (LongWritable, Text)
 */
public static class SumReducer extends Reducer < Text, Text, 
                                                      Text, Text > {

	@Override
	public void reduce(Text key, Iterable < Text > values, Context context) 
		throws IOException, InterruptedException {
		int num_clicks = 0;
		int num_impressions = 0;
		for (Text val : values) {
			String text = val.toString();
			if( text.equals("true") ) num_clicks++;
			num_impressions++;
		}
		double result = num_clicks * 1.0/num_impressions;
		context.write(key, new Text(Double.toString(result)));
	}

}

/**
 * map: (LongWritable, Text) --> (LongWritable, Text)
 * Maps click JSON to [impressionId]--> (adId)
 * NOTE: Keys must implement WritableComparable, values must implement Writable
 */
public static class ClickMapper extends Mapper < LongWritable, Text, Text, Text > {
	@Override
	public void map(LongWritable key, Text val, Context context)
		throws IOException, InterruptedException {
		String entry = val.toString();
		entry = entry.replace('"', '\"');
		Object obj = JSONValue.parse(entry);
		JSONObject json = (JSONObject) obj;
		String adId = (String) json.get("adId");
		String impressionId = (String) json.get("impressionId");
		
		context.write(new Text(impressionId), new Text(adId));
	}

}

/**
 * map: (LongWritable, Text) --> (LongWritable, Text)
 * Maps impression JSON to [impressionId]--> (referrer, adId)
 */
public static class ImpressionMapper extends Mapper < LongWritable, Text, Text, Text > {
	@Override
	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		String jsonString = val.toString();
		String entry = jsonString.replace('"', '\"');
		Object obj = JSONValue.parse(entry);
		JSONObject json = (JSONObject) obj;
		String impressionId = (String) json.get("impressionId");
		String referrer = (String) json.get("referrer");
		String adId = (String) json.get("adId");
		
		context.write(new Text(impressionId), new Text(referrer + " | " + adId));
	}
}

/**
 * reduce: (LongWritable, Text) --> (LongWritable, Text)
 */
public static class ImpressionClickedReducer extends Reducer < Text, Text, 
                                                      Text, Text > {

	@Override
	public void reduce(Text key, Iterable < Text > values, Context context) 
		throws IOException, InterruptedException {

		String referrer = "";
		String adId = "";
		String clicked = "false";
		for (Text val : values) {
			String value = val.toString();
			if(value.contains(" | ")){
				int index = value.indexOf(" | ");
				referrer = value.substring(0, index);
				adId = value.substring(index+3);
			}else{
				clicked = "true";
			}
		}
		
		context.write(new Text(referrer + " | " + adId), new Text(clicked));
	}

}
    
}
    
