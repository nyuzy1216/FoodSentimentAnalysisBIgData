import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;

public class SentimentReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Business ID,Date,Review Sentiment"), NullWritable.get());
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
