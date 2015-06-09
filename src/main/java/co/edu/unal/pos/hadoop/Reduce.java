package co.edu.unal.pos.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import co.edu.unal.pos.model.SaleFact;

import com.google.gson.Gson;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private Gson gson = new Gson();
	private Text saleFactDetailJson = new Text();
     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       int totalProductQuantity = 0;
       int totalProductPrice = 0;
       while (values.hasNext()) {
         SaleFact saleFactDetail = gson.fromJson(values.next().toString(), SaleFact.class);
         totalProductPrice+=saleFactDetail.getPrice();
         totalProductQuantity+=saleFactDetail.getProductQuantity();
       }
       SaleFact saleFactDetail = new SaleFact(null, null, null);
       saleFactDetail.setPrice(totalProductPrice);
       saleFactDetail.setProductQuantity(totalProductQuantity);
       saleFactDetailJson.set(gson.toJson(saleFactDetail));
       output.collect(key, saleFactDetailJson);
     }
}
