package task_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
INPUT
{"0528881469": ("TITLE===Barnes &amp; Noble HDTV Adapter Kit for NOOK HD and NOOK HD+", "RATING===2.4")}

OUTPUT
0528881469,Barnes &amp; Noble HDTV Adapter Kit for NOOK HD and NOOK HD+,2.4
*/
public class ProductNameRatingReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String productTitle = "";
        String productRating = "";

        for (Text value : values) {
            String valueString = value.toString();
            String tokens[] = valueString.split(Constants.PREFIX_DELIMITER);

            if (tokens.length < 2) {
                continue;
            }
            if (tokens[0].equals(Constants.TITLE_PREFIX)) {
                productTitle = tokens[1];
            } else if (tokens[0].equals(Constants.RATING_PREFIX)) {
                productRating = tokens[1];
            }
        }

        if (productTitle.equals("") || productRating.equals("")) {
            return;
        }

        context.write(key, new Text(productTitle + "," + productRating));
    }
}
