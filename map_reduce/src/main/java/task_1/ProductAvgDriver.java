package task_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductAvgDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "ProductAverageRating");

        job.setJarByClass(ProductAvgDriver.class);
        job.setMapperClass(ProductAvgMapper.class);
        job.setCombinerClass(ProductAvgCombiner.class);
        job.setReducerClass(ProductAvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumCountWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        System.exit(ToolRunner.run(conf, new ProductAvgDriver(), args));
    }
}
