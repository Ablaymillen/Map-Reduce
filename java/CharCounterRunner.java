import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// The driver program for mapreduce job.
public class CharCounterRunner extends Configured implements Tool {

    public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CharCounterRunner(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        // Create job
        Job job = Job.getInstance(conf, "Char Counter Job");
        job.setJarByClass(CharCounterRunner.class);

        job.setMapperClass(CharMapper.class);
        job.setReducerClass(CharReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
