import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVParser;
import java.io.IOException;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private CSVParser parser = new CSVParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) return;

        String[] row = parser.parseLine(value.toString());
        if (row.length > 3) {
            String businessId = row[0];
            String date = row[1];
            Text outputKey = new Text(businessId + "," + date + ",");

            try {
                int rating = Integer.parseInt(row[2]); 

                if (rating >= 4) {
                    context.write(new Text(outputKey + "positive"), NullWritable.get());
                } else if (rating == 3) {
                    context.write(new Text(outputKey + "neutral"), NullWritable.get());
                } else if (rating <= 2) {
                    context.write(new Text(outputKey + "negative"), NullWritable.get());
                }
            } catch (NumberFormatException e) {
            }
        }
    }
}
