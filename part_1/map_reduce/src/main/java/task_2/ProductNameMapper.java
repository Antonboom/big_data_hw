package task_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/*
INPUT:
{
    "asin": "0528881469",
    "categories": [["Electronics","Computers & Accessories","Touch Screen Tablet Accessories","Chargers & Adapters"]]
    "description": "HDTV Adapter Kit for NOOK HD and NOOK HD+\nThis handy kit enables you to stream content from...",
    "imUrl":"http://ecx.images-amazon.com/images/I/51RjSETO23L._SX300_.jpg"
    "price": 49.95
    "related": {
        "also_bought": ["B009L7EEZA","B00AGAYQEU","B00AGAS6XW","B00BN1Q5JA","B00AV1UWWY","B00CPV9YOU","1400699169"],
        "bought_together": ["B009L7EEZA"],
        "buy_after_viewing": ["0594481813","0594481902","B009L7EEZA","B00AK2MHEU"]
    },
    "title": "Barnes &amp; Noble HDTV Adapter Kit for NOOK HD and NOOK HD+"
}

OUTPUT:
{
    "0528881469": "TITLE===Barnes &amp; Noble HDTV Adapter Kit for NOOK HD and NOOK HD+"
}
 */
public class ProductNameMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String productID;
        String productTitle;

        try {
            JSONObject productMeta = new JSONObject(value.toString());
            productID = productMeta.getString("asin");
            productTitle = productMeta.getString("title");
        } catch (JSONException exc) {
            return;
        }

        context.write(
            new Text(productID),
            new Text(Constants.TITLE_PREFIX + Constants.PREFIX_DELIMITER + productTitle)
        );
    }
}
