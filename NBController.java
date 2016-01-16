import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class NBController extends Configured implements Tool {    
	public static final double ALPHA = 1.0;
	static enum NB_COUNTERS {
		TOTAL_DOCS,
		VOCABULARY_SIZE,
		UNIQUE_LABELS
    }
	
	public static final String TOTAL_DOCS = "totalDocuments";
	public static final String VOCABULARY_SIZE = "vocabularySize";
	public static final String UNIQUE_LABELS = "uniqueLabels";
	
	
	//For a particular line, add all the attributes of that instance to tokens and return it as an array.
	public static Vector<String> tokenizeDoc(String [] words) {
		Vector<String> tokens = new Vector<String>();
		//Takes in the array list from the a string tokenizer and 
		for (int i = 1; i < words.length; i++) {
			words[i] = words[i].replaceAll("\\W", "");
			//If the words have any white space, remove the space.
			if (words[i].length() > 0) 
			{
				tokens.add(words[i]);
			}
		}
		return tokens;
	}
    
    /**
     * Tokenizes the labels.
     */
	public static Vector<String> tokenizeLabels(String labels) {
		//If there are multiple labels to each class, the value is split and all labels are obtained by splitting them at a ",".
		String [] tokens = labels.split(",");
		Vector<String> classLabels = new Vector<String>();
		for (String token : tokens)
		{
			classLabels.add(token);
		}
		//Returns a Vector of variable length, which contains all the class labels for a particular instance.
		return classLabels;
	}
	
    /**
     * Delete HDFS path so as to run code for many instances.
     */
    
	public static void delete(Configuration conf, Path path) throws IOException
	{
		FileSystem fs = path.getFileSystem(conf); //Return the FileSystem that owns this Path
		if (fs.exists(path)) {
			boolean success = fs.delete(path, true);
			if (!success) {
				System.err.println("Error in deleting path");
			}
		}
	}
    
    public int run(String [] args) throws Exception 
    {
    	Configuration conf = getConf();
    	Configuration classifyConf = new Configuration();
    	
    	//get paths of training, test data and output directory from the configuration
    	Path traindata = new Path(conf.get("train"));
    	Path testdata = new Path(conf.get("test"));
    	Path output = new Path(conf.get("output"));
    	
    	//set number of reducers to 10 by default
    	int numReducers = conf.getInt("reducers", 4);
    	
    	
    	Path distCache = new Path(output.getParent(), "cache");
    	Path model = new Path(output.getParent(), "model");
    	Path joined = new Path(output.getParent(), "joined");
        
    	// Job I:ind the number of instances with each class label in the training data
    	//Delete the path of model if it already exists in the current configuration
    	NBController.delete(conf, model);
    	System.out.println("Job1");
    	//Set up new job called WordTrain
    	Job trainWordJob = new Job(conf, "WordTrain");
    	
    	trainWordJob.setJarByClass(NBController.class);
    	trainWordJob.setNumReduceTasks(numReducers);
    	
    	//Set Mapper and Reducer class for Training Data
    	trainWordJob.setMapperClass(NBTrainWordMapper.class);
    	trainWordJob.setReducerClass(NBTrainWordReducer.class);
    	
    	//input and output formats as plain text files
    	/*
    	 * TextInputFormat parses a plain text file and splits it into parts to be fed to different data nodes.
    	 * TextOutputFormat writes to a plain text file.
    	 */
    	trainWordJob.setInputFormatClass(TextInputFormat.class);
    	trainWordJob.setOutputFormatClass(TextOutputFormat.class);
    	
    	//The <K, V> pair of the mapper will both be of type Text : Ensuring that the Map Outputs and Job Outputs are both of Text class
    	trainWordJob.setMapOutputKeyClass(Text.class);
        trainWordJob.setMapOutputValueClass(Text.class);
        
        //Set the key class for the job output data 
        trainWordJob.setOutputKeyClass(Text.class);
        //Set the value class for job outputs
        trainWordJob.setOutputValueClass(Text.class);
        
        //Add and set input paths and output paths for the job that works on the training data
        FileInputFormat.addInputPath(trainWordJob, traindata);
        //Give an output path for the model that is formed from the training data
        FileOutputFormat.setOutputPath(trainWordJob, model);
        
        
        //Submit the job to the cluster and wait for completion. If it cannot submit job to cluster, return 1.
        if (!trainWordJob.waitForCompletion(true))
        {
        	System.err.println("Couldn't train classifier. Try again");
        	return 1;
        }
        
        //Once the job is done, it gets all counters for the job and selects the one with the property vocabularySize and sets that as the value for Vocabulary Size property
        //The vocabulary size is the number of attributes and their respectives values in the given training data.
        classifyConf.setLong(NBController.VOCABULARY_SIZE,
        		trainWordJob.getCounters().findCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE).getValue());
        
        
        
        // Job II: Find statistic for each class label
        //If distCache already exists, delete the particular directory at that location.
        System.out.println("Job2");
        NBController.delete(conf, distCache);
        
        //Start a new job to train class labels
        Job trainLabelJob = new Job(conf, "trainClassLabels");
        trainLabelJob.setJarByClass(NBController.class);
        trainLabelJob.setNumReduceTasks(numReducers);
        
        //Set Mapper and Reducer classes for the job trainLabelJob
        trainLabelJob.setMapperClass(NBTrainLabelMapper.class);
        trainLabelJob.setReducerClass(NBTrainLabelReducer.class);
        
        //set the input and output formats for plain text files
        trainLabelJob.setInputFormatClass(TextInputFormat.class);
        trainLabelJob.setOutputFormatClass(TextOutputFormat.class);
        
        //Set the output types of key and value to Text and IntWritable respectively for the mapper, both text for the final output
        trainLabelJob.setMapOutputKeyClass(Text.class);
        trainLabelJob.setMapOutputValueClass(IntWritable.class);
        trainLabelJob.setOutputKeyClass(Text.class);
        trainLabelJob.setOutputValueClass(Text.class);
        
        //specify the input and output paths for the job
        //For trainLabelJob, set input path as trainData 
        FileInputFormat.addInputPath(trainLabelJob, traindata);
        //For the trainLabelJob, set the output path as distCache
        FileOutputFormat.setOutputPath(trainLabelJob, distCache);
        
        //check if the job has completed successfully
        if (!trainLabelJob.waitForCompletion(true)) 
        {
        	System.err.println("Couldn't successfully collect class label data. Please try again");
        	return 1;
        }
        
        classifyConf.setLong(NBController.UNIQUE_LABELS,
        		trainLabelJob.getCounters().findCounter(NBController.NB_COUNTERS.UNIQUE_LABELS).getValue());
        classifyConf.setLong(NBController.TOTAL_DOCS,
        		trainLabelJob.getCounters().findCounter(NBController.NB_COUNTERS.TOTAL_DOCS).getValue());
        
		
        
        // Job III: Add the test data set with the model-base already created
        //If the path already exists, delete it 
        NBController.delete(conf, joined);
        System.out.println("Job3");
        
        Job joinJob = new Job(conf, "prepareTestData");
        joinJob.setJarByClass(NBController.class);
        joinJob.setNumReduceTasks(numReducers);
        
        //This class supports MapReduce jobs that have multiple input paths with a different InputFormat and Mapper for each path
        //We add outputs of two Mapper classes - Model and Test to our Join Job.
        MultipleInputs.addInputPath(joinJob, model, KeyValueTextInputFormat.class,
                NBJoinModelMapper.class);
        MultipleInputs.addInputPath(joinJob, testdata, TextInputFormat.class,
                NBJoinTestMapper.class);
        
        //Reducer Class is set
        joinJob.setReducerClass(NBJoinReducer.class);
        joinJob.setOutputFormatClass(TextOutputFormat.class);
        joinJob.setMapOutputKeyClass(Text.class);
        joinJob.setMapOutputValueClass(Text.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);
        
        
        FileOutputFormat.setOutputPath(joinJob, joined);
        
        if (!joinJob.waitForCompletion(true)) 
        {
            System.err.println("Failed to join test data. Try again\n");
            return 1;
        }
        
        // Job IV: Classifies the test data and give accuracy measure
        //If output file already exists, delete it.
        NBController.delete(classifyConf, output);
        
        // Add to the Distributed Cache.
        FileSystem fs = distCache.getFileSystem(classifyConf);
        Path pathPattern = new Path(distCache, "part-r-[0-9]*");
        
        //Get the client side information about the required files
        //Get all the files in the particular pattern
        FileStatus [] list = fs.globStatus(pathPattern);
        
        //Add the obtained files to the distributed cache system
        for (FileStatus status : list)
        {
            DistributedCache.addCacheFile(status.getPath().toUri(), classifyConf);
        }
        
        //Create the fourth job
        System.out.println("Job4");
        Job classify = new Job(classifyConf, "classifyTestData");
        classify.setJarByClass(NBController.class);
        classify.setNumReduceTasks(numReducers);
        
        classify.setMapperClass(NBClassifyMapper.class);
        classify.setReducerClass(NBClassifyReducer.class);
        
        classify.setInputFormatClass(KeyValueTextInputFormat.class);
        classify.setOutputFormatClass(TextOutputFormat.class);
        
        classify.setMapOutputKeyClass(LongWritable.class);
        classify.setMapOutputValueClass(Text.class);
        classify.setOutputKeyClass(LongWritable.class);
        classify.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(classify, joined);
        FileOutputFormat.setOutputPath(classify, output);
        
        /*
         * Report error status if required
         */
        if (!classify.waitForCompletion(true)) 
        {
            System.err.println("ERROR: Classification failed!");
            return 1;
        }
        
        // Last job: manually read through the output file and 
        // sort the list of classification probabilities.
        
        int correct = 0;
        int total = 0;
        pathPattern = new Path(output, "part-r-[0-9]*");
        FileStatus [] results = fs.globStatus(pathPattern);
        for (FileStatus result : results) {
            FSDataInputStream input = fs.open(result.getPath());
            BufferedReader in = new BufferedReader(new InputStreamReader(input));
            String line;
            while ((line = in.readLine()) != null) {
                String [] pieces = line.split("\t");
                correct += (Integer.parseInt(pieces[1]) == 1 ? 1 : 0);
                total++;
            }
            IOUtils.closeStream(in);
        }
        
        System.out.println(String.format("%s/%s, accuracy %.2f", correct, total, ((double)correct / (double)total) * 100.0));
        return 0;
    }

    public static void main(String[] args) throws Exception 
    {
    	NBController NaiveBayesDriver = new NBController();
    	int exitCode = ToolRunner.run(NaiveBayesDriver.getConf(), NaiveBayesDriver, args);
    	System.exit(exitCode);        
    }
}
