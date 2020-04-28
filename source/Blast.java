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

	//Define temporary file for storing data between MapReduce jobs
	Path temp_path = new Path("temp_seeds");

	//Initialization of first job to search for viable seeds
	Configuration conf1 = new Configuration();
			 
	Job job1 = Job.getInstance(conf1, "seed generation job");
	job1.setJarByClass(Blast.class);

	job1.setMapperClass(Blast.OffSetMapper.class);
	job1.setReducerClass(Blast.SumReducer.class);

	FileInputFormat.addInputPath(job1, new Path(args[1]));
	FileOutputFormat.setOutputPath(job1, temp_path);

	//Generate k-mer offset dictionary and serialize into DistributedCache to be shared by Mappers
	FileSystem fs = FileSystem.get(conf1); 
	HashMap<String, Integer> offset_dictionary = new HashMap<String, Integer>();
	Path query_path = new Path(args[0]); 
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(query_path))); 

	String query = "";	
	String line = "";			
    while ((line = reader.readLine()) != null){ 
    	query = query + line;
    } 

    for(int i=0; i<query.length() - 5; i++){
    	System.out.println(query.substring(i, i+6));
    	offset_dictionary.put(query.substring(i, i+6), i);
    }

    FileOutputStream file = new FileOutputStream("./offset_dict.ser");
    ObjectOutputStream out_serial = new ObjectOutputStream(file);

    out_serial.writeObject(offset_dictionary);
    out_serial.close();
    file.close();

	job1.addCacheFile(new URI("./offset_dict.ser"));

	job1.waitForCompletion(true);


	Configuration conf2 = new Configuration();
	Job job2 = Job.getInstance(conf2, "seed alignment job");
	
	job2.setJarByClass(Blast.class);
	job2.setMapperClass(Blast.GlobalAlignmentMapper.class);
	job2.setReducerClass(Blast.FormatterReducer.class);

	FileInputFormat.addInputPath(job2, temp_path);
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	job2.addCacheFile(new URI(query_path + "#query_string"));

	//Add genome_path to DistributedCache
	FileOutputStream output_file = new FileOutputStream("./genome_path");
    PrintStream print_stream = new PrintStream(output_file);
    print_stream.print(args[1]);
    output_file.close();
    print_stream.close();
	job2.addCacheFile(new URI("./genome_path"));

	job2.waitForCompletion(true);

	System.exit(job2.waitForCompletion(true) ? 0 : 1);
}

/**
 * Mapper Class for First Map Reduce Job that looks for k-mers in line
 * map: (LongWritable, Text) --> (LongWritable, Text)
 */
public static class OffSetMapper extends Mapper < LongWritable, Text, 
                                                    LongWritable, Text > {

	
    HashMap<String, Integer> offset_dictionary = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
        
        try { 
            FileSystem fs = FileSystem.get(context.getConfiguration()); 
            FileInputStream file = new FileInputStream("./offset_dict.ser");
            ObjectInputStream input = new ObjectInputStream(file);

            offset_dictionary = (HashMap<String, Integer>) input.readObject();

            file.close();
            input.close();
        } 

        catch (Exception e){ 
            System.out.println(e + " Unable to read cached Query String File"); 
            System.exit(1); 
        } 
	}

	@Override
	public void map(LongWritable key, Text val, Context context)
		throws IOException, InterruptedException {
		String line = val.toString();
		for (int i =0; i< line.length()-5; i++){
			String current_substring = line.substring(i, i+6);
			if(offset_dictionary.containsKey(current_substring)){
				Long query_offset = new Long(offset_dictionary.get(current_substring));
				Long document_offset = key.get();
				Long true_offset = document_offset + i - query_offset;
				true_offset = true_offset - true_offset%20;
				context.write(new LongWritable(true_offset), new Text("1"));
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
	}

}

/**
 * map: (LongWritable, Text) --> (LongWritable, Text)
 * NOTE: Keys must implement WritableComparable, values must implement Writable
 */
public static class GlobalAlignmentMapper extends Mapper < LongWritable, Text, 
                                                    LongWritable, Text > {

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

		    Path genome_path = new Path("./genome_path");
		    BufferedReader genome_reader = new BufferedReader(new InputStreamReader(fs.open(genome_path))); 

			String genome_string = "";	
			line = "";			
		    while ((line = genome_reader.readLine()) != null){ 
		    	genome_string = genome_string + line;
		    } 

		    FSDataInputStream genome_input_stream = fs.open(new Path(genome_string));
		    Long offset = new Long(val.toString().split("\t")[0]);
		    genome_input_stream.seek(offset);

		    genome_reader = new BufferedReader(new InputStreamReader(genome_input_stream)); 

			String genome_subset = "";	
			line = "";			
		    while (genome_subset.length() < query.length() && (line = genome_reader.readLine()) != null){ 
		    	line = line.replace("\n", "");
		    	genome_subset = genome_subset + line;
		    } 

		    System.out.println(genome_subset);
			System.out.println(key + " | " + val);
			System.out.println(offset);
			context.write(key, val);
	}

}

/**
 * reduce: (LongWritable, Text) --> (LongWritable, Text)
 */
public static class FormatterReducer extends Reducer < LongWritable, Text, 
                                                      LongWritable, Text > {

	@Override
	public void reduce(LongWritable key, Iterable < Text > values, Context context) 
		throws IOException, InterruptedException {
		for (Text val: values){
			context.write(key, val);
		}
	}

}
    
}
   