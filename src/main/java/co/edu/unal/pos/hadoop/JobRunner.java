/**
 * 
 */
package co.edu.unal.pos.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.AggregationLevel;
import co.edu.unal.pos.common.constants.HadoopConstants;
import co.edu.unal.pos.common.constants.HadoopPropertiesKeys;
import co.edu.unal.pos.common.constants.PropertiesKeys;
import co.edu.unal.pos.common.properties.PropertiesProvider;
import co.edu.unal.pos.model.SaleFactFilter;

import com.google.gson.Gson;

public class JobRunner {

	private final static Logger logger = Logger.getLogger(JobRunner.class);
	
	private static JobRunner instance;
	
	private Gson gson = new Gson();
	
	private String inputPath;
	
	@SuppressWarnings("deprecation")
	public void runJob(AggregationLevel aggregationLevel,SaleFactFilter saleFactFilter) throws IOException{
	
		logger.info("executing job");
		JobConf jobConf = new JobConf(JobRunner.class);
	     jobConf.setJobName(HadoopConstants.POS_HADOOP_JOB_NAME);
	     logger.info("the filter is "+gson.toJson(saleFactFilter));
	     jobConf.set(HadoopPropertiesKeys.FS_DEFAULT_FS, PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.DFS_URL, HadoopConstants.DFS_URL));
	     if(saleFactFilter!=null){
	    	 jobConf.set(HadoopPropertiesKeys.SALE_FACT_FILTER,gson.toJson(saleFactFilter));
	     }
	     jobConf.setEnum(HadoopPropertiesKeys.AGGREGATION_LEVEL, aggregationLevel);
	     jobConf.setOutputKeyClass(Text.class);
	     jobConf.setOutputValueClass(Text.class);
	
	     jobConf.setMapperClass(Map.class);
	     jobConf.setCombinerClass(Reduce.class);
	     jobConf.setReducerClass(Reduce.class);
	
	     jobConf.setInputFormat(TextInputFormat.class);
	     jobConf.setOutputFormat(TextOutputFormat.class);
	
	     FileSystem fs = FileSystem.get(jobConf);
	     
	     String posHadoopInput = PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.POS_HADOOP_INPUT, HadoopConstants.POS_HADOOP_INPUT);
	     Path posHadoopInputPath=  new Path(posHadoopInput+"/"+inputPath);
	     logger.info("the inputpath is "+posHadoopInputPath);
	     
	     
	    Path posHadoopOutputPath =  new Path(PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.POS_HADOOP_OUTPUT, HadoopConstants.POS_HADOOP_OUTPUT));
	    if(fs.exists(posHadoopOutputPath)){
	    	fs.delete(posHadoopOutputPath);
	    }
	     
	     FileInputFormat.setInputPaths(jobConf,posHadoopInputPath);
	     FileOutputFormat.setOutputPath(jobConf, posHadoopOutputPath);
	
	     JobClient.runJob(jobConf);
	
	     logger.info("finished job");
	}
	
	public void setInputPath(String inputPath){
		this.inputPath = inputPath;
	}
	
	public static JobRunner getInstance(){
		if(instance==null){
			instance = new JobRunner();
		}
		return instance;
	}
	

}