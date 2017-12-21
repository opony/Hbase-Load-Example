package com.lextar;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;
import com.lextar.parser.DvfCsvParser;

public class CsvLoader {

//	public static void main(String[] args) {
//		
//		Configuration config = HBaseConfiguration.create();
//        config.clear();
//        config.set("hbase.zookeeper.quorum", "10.234.72.237");
//        //config.set("hbase.zookeeper.property.clientPort","2181");
//        //config.set("hbase.master", "10.234.72.237:60010");
//		try {
//			HBaseAdmin.checkHBaseAvailable(config);
//			
//			Connection connection = ConnectionFactory.createConnection(config);
//			Table table = connection.getTable(TableName.valueOf("test_table"));
//			byte[] row1 = Bytes.toBytes("row1");
//			Put p = new Put(row1);
//			p.addImmutable("col_fam1".getBytes(), "name".getBytes(), Bytes.toBytes("pony"));
//			table.put(p);
//			
//			//HTable table = new HTable(config, "myLittleHBaseTable");
//			
//			connection.close();
//			
//            System.out.println("HBase is running!");
//            
//            
//		} catch (Exception e) {
//			System.out.println(e);
//			System.out.println("HBase is not running!");
//			
//		}
//	}
	
	private static final Logger logger = LoggerFactory.getLogger(CsvLoader.class);
	
	public static void main(String[] args) throws IOException, ServiceException {
		
		Instant start = Instant.now();
		Path folder = Paths.get("D:/DVF_Temp");
		Path compPath = Paths.get("D:/DVF_Complete");
		
		long totCnt = Files.list(folder).count();
		logger.info("Prepare parse file count : {}", totCnt);
		int fIdx = 0;
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(folder)) {
        	
        	Configuration config = HBaseConfiguration.create();
    		config.clear();
    		config.set("hbase.zookeeper.quorum", "10.234.72.237");
    		HBaseAdmin.checkHBaseAvailable(config);
    		
    		Connection connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf("dvf_table"));
			
		    for (Path path : directoryStream) {
		    	fIdx++;
		    	Instant fiSt = Instant.now();
		    	logger.info("{} / {}, ",fIdx,totCnt,   path.toString());
		    	
		    	//List<DvfData> dvfDataList = dvfParser.Parse(path.toString());
		    	DvfCsvParser dvfParser = new DvfCsvParser();
		    	List<HashMap<String,Object>> dvfDataList = dvfParser.Parse(path.toString());
		    	//Document dvfData = dvfParser.ParseToDocument(path.toString());
		    	if(dvfDataList.size() == 0){
		    		logger.warn("No raw data , then don't insert table");
		    		continue;
		    	}
		    	
		    	//dvfDao.insert(dvfDataList);
		    	
		    	InsertDb(table, dvfParser.getHeaderData(), dvfDataList);
		    	Instant fiEnd = Instant.now();
		    	
		    	logger.info("File done! , {}", Duration.between(fiSt, fiEnd));
		    	
				Files.move(path, compPath.resolve(path.getFileName()));
		    }
		    
		    connection.close();
		} catch (IOException ex) {
			throw ex;
		}
		
	
		Instant end = Instant.now();
		
		logger.info("Done! , {}", Duration.between(start, end));
	}
	
	public static void InsertDb(Table table, HashMap<String,Object> headerMap, List<HashMap<String,Object>> rowList){
		
		try {
			
			
			
			
			
			
			List<Put> puts = new ArrayList<Put>();
			
			
			
			for(HashMap<String,Object> row : rowList){
				String rowKey = (String)headerMap.get("FileName") + (Double)row.get("TEST");
				byte[] keybyte = Bytes.toBytes(rowKey);
				Put p = new Put(keybyte);
				headerMap.forEach((k,v) -> {
					if(v instanceof Double)
						p.addImmutable("header_info".getBytes(), k.getBytes(), Bytes.toBytes((Double)v));
					else
						p.addImmutable("header_info".getBytes(), k.getBytes(), Bytes.toBytes((String)v));
					
				});
				
				row.forEach((k,v) -> {
					if(v instanceof Double)
						p.addImmutable("row_data".getBytes(), k.getBytes(), Bytes.toBytes((Double)v));
					else
						p.addImmutable("row_data".getBytes(), k.getBytes(), Bytes.toBytes((String)v));
				});
				
//				table.put(p);
				puts.add(p);
			}
			
			//HTable table = new HTable(config, "myLittleHBaseTable");
			table.put(puts);
			
			
            
            
            
		} catch (Exception e) {
			System.out.println(e);
			System.out.println("HBase is not running!");
			
		}
	}
	

}
