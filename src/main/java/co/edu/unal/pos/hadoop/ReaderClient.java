package co.edu.unal.pos.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.springframework.data.hadoop.store.input.TextFileReader;

import co.edu.unal.pos.PosHadoopApp;
import co.edu.unal.pos.common.constants.HadoopConstants;
import co.edu.unal.pos.common.constants.HadoopPropertiesKeys;
import co.edu.unal.pos.common.properties.PropertiesProvider;
import co.edu.unal.pos.model.SaleFact;

import com.google.gson.Gson;

public class ReaderClient {

	private final static Logger logger = Logger.getLogger(PosHadoopApp.class);
	
	private static ReaderClient INSTANCE = null;
	
	private static Comparator<SaleFact> salesFactsComparator = new Comparator<SaleFact>() {
		public int compare(SaleFact saleFact1, SaleFact saleFact2) {
			// TODO Auto-generated method stub
			if(saleFact1!=null&&saleFact2!=null&&saleFact1.getPrice()!=null&&saleFact2.getPrice()!=null){
					return saleFact2.getPrice()-saleFact1.getPrice();
			}
			return 0;
		}
	};

	public List<SaleFact> getSaleFacts() throws IOException {
		logger.info("Start reading sales facts from map reduce output");
		Configuration configuration = new Configuration();
		configuration.set(
				HadoopPropertiesKeys.FS_DEFAULT_FS,
				PropertiesProvider.getInstance().getProperty(
						HadoopPropertiesKeys.DFS_URL, HadoopConstants.DFS_URL));
		Path outputPath = new Path(PropertiesProvider.getInstance()
				.getProperty(HadoopPropertiesKeys.POS_HADOOP_OUTPUT,
						HadoopConstants.POS_HADOOP_OUTPUT));
		FileSystem fileSystem = FileSystem.get(configuration);
		RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(
				outputPath, false);
		Gson gson = new Gson();
		List<SaleFact> salesFacts = new ArrayList<SaleFact>();
		while (iterator.hasNext()) {
			LocatedFileStatus locatedFileStatus = iterator.next();
			TextFileReader textFileReader = new TextFileReader(configuration,
					locatedFileStatus.getPath(), null);
			String line = "";
			while ((line = textFileReader.read()) != null) {
				int jsonObjectFinalChar = line.lastIndexOf("}}") + 2;
				SaleFact saleFact = gson.fromJson(
						line.substring(0, jsonObjectFinalChar).trim(),
						SaleFact.class);
				SaleFact saleFactDetail = gson.fromJson(
						line.substring(jsonObjectFinalChar, line.length())
								.trim(), SaleFact.class);
				saleFact.setPrice(saleFactDetail.getPrice());
				saleFact.setProductQuantity(saleFactDetail.getProductQuantity());
				salesFacts.add(saleFact);
//				logger.info(line);
//				logger.info(gson.toJson(saleFact));
			}
			textFileReader.close();
		}
		Collections.sort(salesFacts, salesFactsComparator);
		logger.info("End reading sales facts from map reduce output");
		return salesFacts;
	}

	public static ReaderClient getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new ReaderClient();
		}
		return INSTANCE;
	}

	public static void main(String[] args) throws IOException {
		ReaderClient.getInstance().getSaleFacts();
	}
}
