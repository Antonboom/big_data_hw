package task_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
INPUT:
0528881469,2.4

OUTPUT:
{
    "0528881469": "RATING===2.4"
}
 */
public class ProductRatingMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String productIDRating[] = value.toString().split(",");

        String productID = productIDRating[0].trim();
        String productRating = productIDRating[1].trim();

        context.write(
            new Text(productID),
            new Text(Constants.RATING_PREFIX + Constants.PREFIX_DELIMITER + productRating)
        );
    }
}
