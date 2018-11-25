package task_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
INPUT:
{
    "0528881469": ("5.0:1","1.0:1","3.0:1","2.0:1", "1.0:1")
}

OUTPUT:
{
    "0528881469": "12.0:5"
}
 */
public class ProductAvgCombiner extends Reducer<Text,SumCountWritable,Text,SumCountWritable> {

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

        context.write(key, result);
    }
}
