package task_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
INPUT:
0528881469,Barnes &amp; Noble HDTV Adapter Kit for NOOK HD and NOOK HD+,2.4

OUTPUT:
{
    "0528881469": "Barnes &amp; Noble HDTV Adapter Kit for NOOK HD and NOOK HD+,2.4"
}
*/
public class ProductFilteredMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String tokens[] = value.toString().split(",");

        String productTitle = tokens[1];
        String filterWord = context.getConfiguration().get(Constants.FILTER_CONFIG_KEY);
        if (productTitle.toLowerCase().contains(filterWord.toLowerCase())) {
            context.write(new Text(tokens[0]), new Text(tokens[1] + "," + tokens[2]));
        }
    }
}
