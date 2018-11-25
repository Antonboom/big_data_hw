package task_1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProductAvgReducer extends Reducer<Text,SumCountWritable,Text,DoubleWritable> {

    private SumCountWritable result = new SumCountWritable();

    public void reduce(Text key, Iterable<SumCountWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;
        int count = 0;

        for (SumCountWritable val : values) {
            sum += val.getSum();
            count += val.getCount();
        }

        result.set(sum, count);

        //System.out.println("Reducer");
        //System.out.println(result.toString());

        double average = sum / count;

        System.out.println(key + " : " + average);

        context.write(key, new DoubleWritable(average));
    }
}