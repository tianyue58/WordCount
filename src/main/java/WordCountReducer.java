import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCountReducer  extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text txt, Iterator<IntWritable> times,OutputCollector<Text,IntWritable> output,
                       Reporter reporter) throws IOException {
        int sum = 0;
        while (times.hasNext()) {
            sum += times.next().get();
        }
        output.collect(txt,new IntWritable(sum));
    }
}
