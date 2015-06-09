package co.edu.unal.pos.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.AggregationLevel;
import co.edu.unal.pos.common.constants.HadoopPropertiesKeys;
import co.edu.unal.pos.common.constants.IntegerFilterOperator;
import co.edu.unal.pos.model.SaleFact;
import co.edu.unal.pos.model.SaleFactFilter;

import com.google.gson.Gson;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	private final static Logger logger = Logger.getLogger(Map.class);
     
     private Text saleFactJson = new Text();
     private Text saleFactDetailJson = new Text();
     private Gson gson = new Gson();
     private AggregationLevel aggregationLevel = AggregationLevel.YEAR;
     private SaleFactFilter saleFactFilter = null;
     @Override
    public void configure(JobConf job) {
    	super.configure(job);
    	aggregationLevel = job.getEnum(HadoopPropertiesKeys.AGGREGATION_LEVEL, AggregationLevel.YEAR);
    	saleFactFilter = gson.fromJson(job.get(HadoopPropertiesKeys.SALE_FACT_FILTER),SaleFactFilter.class);
    	logger.info("saleFactFilter="+job.get(HadoopPropertiesKeys.SALE_FACT_FILTER));
    }
     
     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       String line = value.toString();
       SaleFact saleFact = gson.fromJson(line, SaleFact.class);
       if(saleFact.getTimeDimension()!=null){
           boolean processLine = applyFilter(saleFact);
           if(processLine){
               if(aggregationLevel.equals(AggregationLevel.YEAR)){
            	   saleFact.getTimeDimension().setMonth(null);
            	   saleFact.getTimeDimension().setDay(null);
               }
               else if(aggregationLevel.equals(AggregationLevel.MONTH)){
           	   saleFact.getTimeDimension().setDay(null);
              }

               SaleFact saleFactDetail = new SaleFact(null,null,null);
              saleFactDetail.setPrice(saleFact.getPrice());
              saleFactDetail.setProductQuantity(saleFact.getProductQuantity());
              
              saleFact.setProductQuantity(null);
              saleFact.setPrice(null);
              
              saleFactJson.set(gson.toJson(saleFact));
              saleFactDetailJson.set(gson.toJson(saleFactDetail));
              
              output.collect(saleFactJson,saleFactDetailJson);  
//              logger.info(line);
           }   
       }
   }

	private boolean applyFilter(SaleFact saleFact) {
		boolean processLine = true;
		if(saleFactFilter!=null){
			
			boolean includeById = saleFactFilter.getId()==null||(saleFact.getProductDimension().getId().equals(saleFactFilter.getId()));
			boolean includeByDescription = (saleFactFilter.getDescription()==null||saleFact.getProductDimension().getDescription()==null)
					||(saleFact.getProductDimension().getDescription().toLowerCase().contains(saleFactFilter.getDescription().toLowerCase()));
			
        	boolean includeByYear =  applyOperator(saleFact.getTimeDimension().getYear(),saleFactFilter.getYear(),saleFactFilter.getYearOperator());
    		boolean includeByMonth =  applyOperator(saleFact.getTimeDimension().getMonth(),saleFactFilter.getMonth(),saleFactFilter.getMonthOperator());
        	boolean includeByDay =  applyOperator(saleFact.getTimeDimension().getDay(),saleFactFilter.getDay(),saleFactFilter.getDayOperator());
        	processLine = includeById&&includeByDescription&&includeByYear&&includeByMonth&&includeByDay;
        }
		return processLine;
	}

	private boolean applyOperator(Integer integer1, Integer integer2,
			IntegerFilterOperator integerFilterOperator) {
		if(integer1!=null&&integer2!=null){
			if(integerFilterOperator.equals(IntegerFilterOperator.EQUALS)){
				return integer1.compareTo(integer2)==0;
			}			
			if(integerFilterOperator.equals(IntegerFilterOperator.LESS)){
				return integer1.compareTo(integer2)<0;
			}
			if(integerFilterOperator.equals(IntegerFilterOperator.MORE)){
				return integer1.compareTo(integer2)>0;
			}if(integerFilterOperator.equals(IntegerFilterOperator.LESS_OR_EQUALS)){
				return integer1.compareTo(integer2)<=0;
			}
			if(integerFilterOperator.equals(IntegerFilterOperator.MORE_OR_EQUALS)){
				return integer1.compareTo(integer2)>=0;
			}
		}
		return true;
	}
}