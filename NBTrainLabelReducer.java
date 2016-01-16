import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Tabulates some counts under each label. The output will be serialized
 * in the DistributedCache for access by the classifier after this step.
 */
public class NBTrainLabelReducer extends Reducer<Text, IntWritable, Text, Text>
{

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws InterruptedException, IOException 
    {
        // Each time this Reducer is invoked, that means we have another unique label.
    	//Increment the number of class labels - a global counter
        context.getCounter(NBController.NB_COUNTERS.UNIQUE_LABELS).increment(1);
        //System.out.println(context.getCounter(NBController.NB_COUNTERS.UNIQUE_LABELS).getValue()+"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
        
        long pY = 0;
        long pYW = 0;
        
        	
        
        for (IntWritable value : values) 
        {
        	// Increment the global counter (Y = *)
        	//Increment the global counter that holds the number of attribute values
            context.getCounter(NBController.NB_COUNTERS.TOTAL_DOCS).increment(1);
            
            // Increment the number of instances with this class label (Y = y)
            pY++;
            
            // Increment the number of attributes with the given label and the no of attributes
            pYW += value.get();
        }
        
        System.out.println("pY = " + pY + "        pYW = " + pYW + "\n\n\n\n\n\n");
        
        // Write out the results in the format:
        // <label Y> {<# of instances with class label Y>:<# of attributes under label Y>} 
        context.write(key, new Text(String.format("%s:%s", pY, pYW)));
    }
}
