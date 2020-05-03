import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;
import java.util.*;

/**
 * BLAST implemented as Map Reduce job that takes query string and searchable genome and returns similar substrings
 */
public class Blast {

public static void main(String[] args) throws Exception {
	if (args.length < 3) {
		System.err.println("Error: Wrong number of parameters");
		System.err.println("Expected: [in query file] [in genome file] [out]");
		System.exit(1);
	}
	final long start_time = System.currentTimeMillis();

	System.out.println("I'm starting");
	//Define temporary file for storing data between MapReduce jobs
	Path temp_path = new Path("temp_seeds");

	//Initialization of first job to search for viable seeds
	Configuration conf1 = new Configuration();
			 
	//Job for finding seeds where we will run more expensive Dynamic programming string matching
	Job job1 = Job.getInstance(conf1, "seed generation job");
	job1.setJarByClass(Blast.class);

	job1.setMapperClass(Blast.OffSetMapper.class);
	job1.setReducerClass(Blast.SumReducer.class);

	FileInputFormat.addInputPath(job1, new Path(args[1]));
	FileOutputFormat.setOutputPath(job1, temp_path);

	job1.waitForCompletion(true);

	final long mid_time = System.currentTimeMillis();

	//Job runs Needleman-Wunsch dynamic programming algorithm on zones with high seed values
	Configuration conf2 = new Configuration();
	Job job2 = Job.getInstance(conf2, "seed alignment job");
	
	job2.setJarByClass(Blast.class);
	job2.setMapperClass(Blast.GlobalAlignmentMapper.class);
	job2.setReducerClass(Blast.FormatterReducer.class);
	job2.setMapOutputKeyClass(Text.class);

	FileInputFormat.addInputPath(job2, temp_path);
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	//job2.addCacheFile(new URI(query_path + "#query_string"));

	//Add genome_path to DistributedCache
	FileOutputStream output_file = new FileOutputStream("./genome_path");
    PrintStream print_stream = new PrintStream(output_file);
    print_stream.print(args[1]);
    output_file.close();
    print_stream.close();
	job2.addCacheFile(new URI("./genome_path"));

	//job2.waitForCompletion(true);

	final long end_time = System.currentTimeMillis();
	System.out.println("First Job runtime: " + (mid_time - start_time));
	System.out.println("Second Job runtime: " + (end_time - mid_time));
	System.out.println("Total runtime: " + (end_time - start_time));
}

/**
 * Mapper Class for First Map Reduce Job that looks for k-mers in line
 * map: (LongWritable, Text) --> (LongWritable, Text)
 */
public static class OffSetMapper extends Mapper < LongWritable, Text, 
                                                    LongWritable, Text > {

	
    HashMap<String, List<Integer>> offset_dictionary = null;

    //Method runs once for each Mapper node allowing us to just read in serialized hash table once
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
        
        try { 
        	//System.out.println("I'm setting up");

        	FileSystem fs = FileSystem.get(context.getConfiguration()); 
        	Path query_path = new Path("hdfs://172.31.57.12:9000/user/ubuntu/query");
        	/*
        	BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(query_path))); 
			String query = "";	
			String line = "";			
		    while ((line = reader.readLine()) != null){ 
		    	query = query + line;
		    } 

		    for(int i=0; i<query.length() - 5; i++){
		    	String key = query.substring(i, i+6);
		    	if(!offset_dictionary.containsKey(key)){
		    		offset_dictionary.put(key, new ArrayList<Integer>());
		    	}
		    	offset_dictionary.get(key).add(i);
		    }
		    */
    
        }catch (Exception e){ 
            System.out.println(e + " Unable to read cached Query String File"); 
            System.exit(1); 
        } 

        super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text val, Context context)
		throws IOException, InterruptedException {
		String line = val.toString();
		for (int i =0; i< line.length()-5; i++){
			String current_substring = line.substring(i, i+6);
			if(offset_dictionary.containsKey(current_substring)){
				for( Integer saved_offset : offset_dictionary.get(current_substring)){
					Long query_offset = new Long(saved_offset);
					Long document_offset = key.get();
					Long true_offset = document_offset + i - query_offset;
					true_offset = true_offset - true_offset%20;
					context.write(new LongWritable(true_offset), new Text("1"));
				}
			}
		}
	}
}

/**
 * Reducer class for first job that just totals number of intermediate emissions with same key
 * reduce: (LongWritable, Text) --> (LongWritable, Text)
 */
public static class SumReducer extends Reducer < LongWritable, Text, 
                                                      LongWritable, Text > {

	@Override
	public void reduce(LongWritable key, Iterable < Text > values, Context context) 
		throws IOException, InterruptedException {

		int total = 0;
		for (Text val : values) {
			total++;
		}

		int cutoff_score = 1;
		if( total > cutoff_score){
			context.write(key, new Text(String.valueOf(total)));
		}
		System.out.println(key + String.valueOf(total));
	}

}

/**
 * Mapper functions that takes offset zone with reasonable density and runs Needleman-Wunsh
 * map: (LongWritable, Text) --> (Text, Text)
 */
public static class GlobalAlignmentMapper extends Mapper < LongWritable, Text, 
                                                    Text, Text > {
    //Method for testing behavior of generate_global_memo
    public void print_2d(int[][] memo){
    	for(int i=0; i<memo.length; i++){
    		for(int j=0; j<memo[0].length; j++){
    			System.out.print(memo[i][j] + ",");
    		}
    		System.out.println("");
    	}
    }

    //Method for generating dynamic programming memo of alignment score
    //Employs gap penalty of -2 for gap and matching score of 1 for match and -1 for mismatch
    public int[][] generate_global_memo(String query, String genome){
    	int[][] memo = new int[genome.length() + 1][query.length() + 1];

    	for(int i=0; i< genome.length() + 1; i++){
    		memo[i][0] = -2*i;
    	}
    	for(int j=0; j< query.length() + 1; j++){
    		memo[0][j] = -2*j;
    	}

    	for(int i=1; i< genome.length() + 1; i++){
    		for(int j=1; j< query.length() + 1; j++){
    			int match = -1;
    			if(query.charAt(j-1) == genome.charAt(i-1)){
    				match = 1;
				}
				memo[i][j] = Math.max(memo[i-1][j] - 2, Math.max(memo[i][j-1] -2, memo[i-1][j-1] + match));
    		}
    	}
    	return memo;
    }

    //Performs traceback through memo to determine globally optimal alignment
    public String[] global_traceback(int[][] memo, String query, String genome){
    	String resultQ = "";
    	String resultG = "";

    	int i = genome.length();
    	int j = query.length();

    	while(i >=0 && j >= 0){
    		if( i == 0 && j == 0){
    			break;
    		}else if (i == 0){
    			resultG = "-" + resultG;
    			resultQ = query.charAt(j-1) + resultQ;
    			j--;
    		}else if (j == 0){
    			resultQ = "-" + resultQ;
    			resultG = genome.charAt(i-1) + resultG;
    			i--;
    		}else{
    			if(memo[i][j] == memo[i-1][j] - 2){
    				resultQ = "-" + resultQ;
    				resultG = genome.charAt(i-1) + resultG;
    				i--;
    			}else if( (memo[i][j] == memo[i-1][j-1]-1 && genome.charAt(i-1) != query.charAt(j-1)) || (memo[i][j] == memo[i-1][j-1]+1 && genome.charAt(i-1) == query.charAt(j-1))){
    				resultQ = query.charAt(j-1) + resultQ;
    				resultG = genome.charAt(i-1) + resultG;
    				i--;
    				j--;
    			}else{
    				resultG = "-" + resultG;
    				resultQ = query.charAt(j-1) + resultQ;
    				j--;
    			}
    		}
    	}

    	//Put strings into a list so we can return three different values here
    	String[] return_val = new String[3];
    	return_val[0] = resultQ;
    	return_val[1] = resultG;
    	return_val[2] = Integer.toString(memo[genome.length()][query.length()]);

    	return return_val;
    }

	@Override
	public void map(LongWritable key, Text val, Context context)
		throws IOException, InterruptedException {
			//Get Query string
            FileSystem fs = FileSystem.get(context.getConfiguration()); 
            Path query_string_path = new Path("./query_string");
		    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(query_string_path))); 

			String query = "";	
			String line = "";			
		    while ((line = reader.readLine()) != null){ 
		    	query = query + line;
		    } 

		    //Get path to genome in file system
		    Path genome_path = new Path("./genome_path");
		    BufferedReader genome_reader = new BufferedReader(new InputStreamReader(fs.open(genome_path))); 

			String genome_string = "";	
			line = "";			
		    while ((line = genome_reader.readLine()) != null){ 
		    	genome_string = genome_string + line;
		    } 

		    //Find position in genome using FSDataInputStream because of its random access property through seek
		    FSDataInputStream genome_input_stream = fs.open(new Path(genome_string));
		    Long offset = new Long(val.toString().split("\t")[0]);
		    try{
		    	genome_input_stream.seek(offset + offset/70);
		    }catch(Exception e){
		    	return;
		    }

		    genome_reader = new BufferedReader(new InputStreamReader(genome_input_stream)); 

			String genome_substring = "";	
			line = "";			
		    while (genome_substring.length() < query.length() && (line = genome_reader.readLine()) != null){ 
		    	line = line.replace("\n", "");
		    	genome_substring = genome_substring + line;
		    } 

		    if (genome_substring.length() > query.length()  + 10){
		    	genome_substring = genome_substring.substring(0, query.length() + 10);
		    }

		    int[][] memo = generate_global_memo(query, genome_substring);
		    String[] result = global_traceback(memo, query, genome_substring);


		    if(Integer.parseInt(result[2]) > -15){
		    	Text new_key = new Text("Aligned Query:\n" + result[0]);
				Text new_val = new Text("\nAligned Substring of Genome:\n" + result[1] + "\n\nAlignment Score: " + result[2]);
				context.write(new_key, new_val);
		    }
	}

}

/**
 * reduce: (LongWritable, Text) --> (LongWritable, Text)
 */
public static class FormatterReducer extends Reducer < Text, Text, 
                                                      Text, Text > {

	@Override
	public void reduce(Text key, Iterable < Text > values, Context context) 
		throws IOException, InterruptedException {
		for (Text val: values){
			context.write(key, val);
		}
	}

}
    
}
   