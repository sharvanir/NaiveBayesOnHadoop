import java.io.IOException;
import java.util.Vector;

//Required hadoop packages
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NBTrainWordMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException 
	{
		//Map gets an input of value, where each value is a line.
		String[] words = value.toString().split("\\s+");
		//Each line is split into parts by a string tokenizer
		Vector<String> labels = NBController.tokenizeLabels(words[0]);
		//labels now contains the associated class of that particular instance
		Vector<String> text = NBController.tokenizeDoc(words);
		//text contains the attributes
		for (String label : labels)
		{
			for (String word : text) 
			{
				//System.out.println(word+":"+label);
				context.write(new Text(word), new Text(label));
			}
		}
	}
}