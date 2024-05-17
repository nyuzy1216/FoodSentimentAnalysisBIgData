import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class YelpCleanReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Write the header at the start of the file
        context.write(NullWritable.get(), new Text("business_id,date,stars,cool,useful,funny,highRating"));
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
