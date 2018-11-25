package task_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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

        //System.out.println("Combiner");
        //System.out.println(result.toString());

        context.write(key, result);
    }
}
