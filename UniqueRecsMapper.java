import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UniqueRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private Map<String, Integer> columnMap = new HashMap<>();
    private boolean isHeaderProcessed = false;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = new CSVParser().parseLine(line);

        // Process header to find column indices
        if (!isHeaderProcessed) {
            for (int i = 0; i < parts.length; i++) {
                columnMap.put(parts[i], i);
            }
            isHeaderProcessed = true;
            return;
        }

        // Get data from columns by name
        String businessId = parts[columnMap.get("business_id")];
        String stars = parts[columnMap.get("stars")];
        String date = parts[columnMap.get("date")];

        Text outputKey = new Text("business_id:" + businessId + ", stars:" + stars + ", date:" + date);
        context.write(outputKey, one);
    }
}
