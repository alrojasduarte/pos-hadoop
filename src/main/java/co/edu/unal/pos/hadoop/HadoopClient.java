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


public class HadoopClient {

	private final static Logger logger = Logger.getLogger(HadoopClient.class);
	
	private static HadoopClient INSTANCE;
	private Configuration configuration = new Configuration();
	private String currentPath = null;
	private TextFileWriter writer;
	private int numberOfWrittenEntities = 0;
	
	private HadoopClient(){
		 configuration.set(HadoopPropertiesKeys.FS_DEFAULT_FS, PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.DFS_URL, HadoopConstants.DFS_URL));
	}
	
	public void openPath() throws IOException{
		closePath();		
		currentPath = PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.PATH_PATTERN, HadoopConstants.PATH_PATTERN);
		currentPath = currentPath.replaceAll(HadoopConstants.PATH_UUID_WILDCARD, UUID.randomUUID().toString());
		String posHadopHome = PropertiesProvider.getInstance().getProperty(HadoopPropertiesKeys.POS_HADOOP_HOME, HadoopConstants.POS_HADOOP_HOME);
		Path path = new Path(posHadopHome+"/"+currentPath);
		logger.info("openning "+path);
		writer = new TextFileWriter(configuration, path, null);
		logger.info("openned "+path);
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

	public static HadoopClient getInstance(){
		if(INSTANCE==null){
			INSTANCE = new HadoopClient();
		}
		return INSTANCE;
	}
	
}