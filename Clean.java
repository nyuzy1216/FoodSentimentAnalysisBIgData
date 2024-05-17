import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Clean {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Clean");
        job.setJarByClass(Clean.class);
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(CleanReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.addFileToClassPath(new Path("opencsv.jar"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
