import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NBTrainWordReducer extends Reducer<Text, Text, Text, Text> 
{
    
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException 
	{
            
		// Update the counter to indicate the size of the vocabulary.
		// Whenever the reduce function is called, Vocabulary Size is increased, indicating progress of the MapReduce job.
		context.getCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE).increment(1);
		
		// Loop through the labels.
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		
		for (Text label : values) {
			String labelKey = label.toString();
			//For each key, the number of instances of value are calculated.
			counts.put(labelKey, 
					new Integer(counts.containsKey(labelKey) ? counts.get(labelKey).intValue() + 1 : 1));
		}
		
		
		//A string builder is a mutable sequence of characters, initially of size 16. THe value is incremented over time.
		StringBuilder outKey = new StringBuilder();
		
		//The outKey StringBuilder has a Strings appended to it of the format label:countValue
		for (String label : counts.keySet()) {
			outKey.append(String.format("%s:%s ", label, counts.get(label).intValue()));
			//System.out.println(label.toString()+":"+counts.get(label).intValue());
		}
		//System.out.println(outKey);
		/*for (String label : counts.keySet()) {
			System.out.println(label + " " + counts.get(label));
		}*/
		
		//System.out.println(outKey+"\n\n\n\n\n\n\n\n\n\n\n\n\n");		

		// Write out the Map associated with the word.
		context.write(key, new Text(outKey.toString().trim()));
	}
}