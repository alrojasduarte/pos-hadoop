package co.edu.unal.pos.hadoop;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.data.hadoop.store.output.TextFileWriter;

import co.edu.unal.pos.common.constants.HadoopConstants;
import co.edu.unal.pos.common.constants.HadoopPropertiesKeys;
import co.edu.unal.pos.common.properties.PropertiesProvider;


public class WriterClient {

	private final static Logger logger = Logger.getLogger(WriterClient.class);
	
	private static WriterClient INSTANCE;
	private Configuration configuration = new Configuration();
	private String currentPath = null;
	private TextFileWriter writer;
	private int numberOfWrittenEntities = 0;
	
	public void openPath() throws IOException{
		closePath();
		configuration.set(HadoopPropertiesKeys.FS_DEFAULT_FS, PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.DFS_URL, HadoopConstants.DFS_URL));
		currentPath = PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.PATH_PATTERN, HadoopConstants.PATH_PATTERN);
		currentPath = currentPath.replaceAll(HadoopConstants.PATH_UUID_WILDCARD, UUID.randomUUID().toString());
		String posHadopInput = PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.POS_HADOOP_INPUT, HadoopConstants.POS_HADOOP_INPUT);
		Path inputPath = new Path(posHadopInput+"/"+currentPath);
		logger.info("openning "+inputPath);
//		FileSystem fs = FileSystem.get(configuration);
//		fs.create(inputPath);
		writer = new TextFileWriter(configuration, inputPath, null);
		logger.info("openned "+inputPath);
	}
	
	public void write(String entity) throws IOException{
		synchronized (INSTANCE) {
			writer.write(entity);
			numberOfWrittenEntities++;
			int flushSize = PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.FILE_FLUSH_SIZE, HadoopConstants.FILE_FLUSH_SIZE);
			if(numberOfWrittenEntities%flushSize==0){
				writer.flush();
			}
		}
	}
	
	public void closePath() throws IOException{
		currentPath = null;
		numberOfWrittenEntities = 0;
		if(writer!=null){
			writer.close();
			writer.flush();
		}		
	}
	
	
	public String getCurrentPath() {
		return currentPath;
	}

	public static WriterClient getInstance(){
		if(INSTANCE==null){
			INSTANCE = new WriterClient();
		}
		return INSTANCE;
	}
	
}