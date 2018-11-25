package task_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

/*
INPUT:
{
    "asin": "0528881469",
    "helpful": [0, 0],
    "overall": 5.0,
    "reviewText": "We got this GPS for my husband who is an (OTR) over the road trucker.",
    "reviewTime": "06 2, 2013"
    "reviewerID": "AO94DHGC771SJ",
    "reviewerName": "amazdnu",
    "summary": "Gotta have GPS!",
    "unixReviewTime": 1370131200
}

OUTPUT:
{
    "0528881469": "5.0 : 1"
}
*/
public class ProductAvgMapper extends Mapper<Object, Text, Text, SumCountWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject json = new JSONObject(value.toString());

        String product = json.getString("asin");
        double rating = json.getDouble("overall");

        context.write(new Text(product), new SumCountWritable(rating, 1));
    }
}
