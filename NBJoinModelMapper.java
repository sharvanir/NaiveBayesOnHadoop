import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Acts as an idenity mapper just for the purpose of sending it through to the other reducer
 */
public class NBJoinModelMapper extends Mapper<Text, Text, Text, Text> 
{
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException 
	{
		//emit out the given key and value
		System.out.println(key.toString()+ " "+value.toString()+"\n");
		context.write(key, value);
	}
}
