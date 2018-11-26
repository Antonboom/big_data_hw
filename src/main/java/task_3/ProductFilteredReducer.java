package task_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
INPUT:
{
    "0528881469": ("Barnes &amp; Noble HDTV Adapter Kit for NOOK HD and NOOK HD+,2.4")
}

OUTPUT:
0528881469,Barnes &amp; Noble HDTV Adapter Kit for NOOK HD and NOOK HD+,2.4
*/
public class ProductFilteredReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
