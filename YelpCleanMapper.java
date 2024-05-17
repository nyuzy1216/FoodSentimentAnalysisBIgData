import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVParser;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class YelpCleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private CSVParser parser = new CSVParser();
    private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat outputDateFormat = new SimpleDateFormat("MM-dd-yyyy");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) { 
            return;
        }

        String[] row = parser.parseLine(value.toString());
        if (row.length > 8) { 
            if ("NULL".equals(row[0]) || "#NAME?".equals(row[0]) ||
                "NULL".equals(row[1]) || "#NAME?".equals(row[1]) ||
                "NULL".equals(row[3]) || "#NAME?".equals(row[3]) ||
                "NULL".equals(row[6]) || "#NAME?".equals(row[6]) ||
                "NULL".equals(row[7]) || "#NAME?".equals(row[7]) ||
                "NULL".equals(row[8]) || "#NAME?".equals(row[8])) {
                return; 
            }

            String formattedDate;
            try {
                Date date = inputDateFormat.parse(row[1]);
                formattedDate = outputDateFormat.format(date);
            } catch (ParseException e) {
                formattedDate = "Invalid Date"; 
            }

            String normalizedUserId = row[3].toLowerCase().trim();

            int stars = Integer.parseInt(row[6]);
            int highRating = stars >= 4 ? 1 : 0;

            String cleanedRow = String.join(",", row[0], formattedDate, normalizedUserId, row[6], row[7], row[8], String.valueOf(highRating));
            context.write(NullWritable.get(), new Text(cleanedRow));
        }
    }
}

