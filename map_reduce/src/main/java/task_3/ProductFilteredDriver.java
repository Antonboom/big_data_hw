package task_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductFilteredDriver  extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "ProductFiltered");

        job.setJarByClass(ProductFilteredDriver.class);
        job.setMapperClass(ProductFilteredMapper.class);
        job.setReducerClass(ProductFilteredReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(Constants.FILTER_CONFIG_KEY, args[2]);
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        System.exit(ToolRunner.run(conf, new ProductFilteredDriver(), args));
    }
}
