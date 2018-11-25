package task_1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
INPUT:
{
    "0528881469": ("12.0:5",)
}

OUTPUT:
0528881469 : 2.4
 */
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

        double average = sum / count;

        System.out.println(key + "," + average);

        context.write(key, new DoubleWritable(average));
    }
}