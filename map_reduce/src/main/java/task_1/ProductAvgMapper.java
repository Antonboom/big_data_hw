package task_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

/*
INPUT:
0528881469,2.4

OUTPUT:
{
    "0528881469": "5.0 : 1"
}
*/
public class ProductAvgMapper extends Mapper<Object, Text, Text, SumCountWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject productData = new JSONObject(value.toString());

        String productID = productData.getString("asin");
        double productRating = productData.getDouble("overall");

        context.write(new Text(productID), new SumCountWritable(productRating, 1));
    }
}
