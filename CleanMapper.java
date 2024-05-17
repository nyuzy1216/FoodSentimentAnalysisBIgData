import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVParser;
import java.io.IOException;

public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private CSVParser parser = new CSVParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header of CSV file
        if (key.get() == 0) {
            return;
        }

        String[] row = parser.parseLine(value.toString());
        // Ensure there are enough columns
        if (row.length > 5) {
            // Skip rows with 'NULL' or '#NAME?'
            if (row[0].equals("NULL") || row[0].equals("#NAME?") ||
                row[1].equals("NULL") || row[1].equals("#NAME?") ||
                row[3].equals("NULL") || row[3].equals("#NAME?") ||
                row[6].equals("NULL") || row[6].equals("#NAME?") ||
                row[7].equals("NULL") || row[7].equals("#NAME?") ||
                row[8].equals("NULL") || row[8].equals("#NAME?")) {
                return; // Skip this row
            }

            String cleanedRow = row[0] + "," + row[1] + "," + row[3] + "," +
                                row[6] + "," + row[7] + "," + row[8];
            context.write(NullWritable.get(), new Text(cleanedRow));
        }
    }
}
