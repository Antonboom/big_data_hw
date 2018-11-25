package task_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

public class ProductAvgMapper extends Mapper<Object, Text, Text, SumCountWritable> {

    private Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        JSONObject json = new JSONObject(value.toString());

        String prod = json.getString("asin");
        double rating = json.getDouble("overall");

        //System.out.println(rating);

        context.write(new Text(prod), new SumCountWritable(rating, 1));
    }
}
