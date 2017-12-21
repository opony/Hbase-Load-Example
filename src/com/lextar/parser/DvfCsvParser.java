package com.lextar.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.math.NumberUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class DvfCsvParser {
	private static final Logger logger = LoggerFactory.getLogger(DvfCsvParser.class);
	
	private HashMap<String,Object> headerDataMap = new HashMap<String,Object>();
	
	enum ParseStep{
		Init,
		Header,
		Summary,
		Raw
	}
	
	public HashMap<String,Object> getHeaderData(){
		return headerDataMap;
	}
	
	public List<HashMap<String,Object>> Parse(String path) throws IOException {
		
		ParseStep currStep = ParseStep.Init;
		String[] headers = null;
		String[] strs = path.split("#");
		
		double testTime = Double.parseDouble(strs[1]);
		List<HashMap<String,Object>> dvfList = new ArrayList<HashMap<String,Object>>(); 
		
		int lineNo = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
            	lineNo++;
            	
            	if(line.startsWith("FileID,")){
            		currStep = ParseStep.Header;
            	}
            	
                if(line.startsWith("TEST,Bin,BIN_CODE"))
                {
                	headers = getColumnHeader(line);
                	currStep = ParseStep.Raw;
                	continue;
                }
                
                //SUMITEM,Bin
                if(line.startsWith("SUMITEM,Bin")){
                	currStep = ParseStep.Summary;
                	continue;
                }
                
                if(line.startsWith("<<<") || line.trim() == "")
                	continue;
                
                if(currStep == ParseStep.Header){
                	parseHeaderInfo(line);
                }
                
                if(currStep == ParseStep.Raw)
                {
                	HashMap<String,Object> map = convertToHashMap(headers, line);
                	
                	
                	if(map != null){
                		map.put("TEST_TIME", testTime);
//                		String moNo = (String)map.get("WO");
//                		String prod = DataProxy.getProductID(moNo);
//                		
//                		map.put("PRODUCT_NO", prod);
                		dvfList.add(map);
                	}
                		
                		
                	
                }
            }
            
            
        } catch (Exception e) {
        	logger.error("Parse line no {} error", lineNo);
            throw e;
        }
		
		return dvfList;
	}
	
	private String[] getColumnHeader(String line)
	{
		return line.toUpperCase().split(",");
	}
	
	private void parseHeaderInfo(String line)
	{
		String[] strs = line.split(",",-1);
		String val = strs[2].replace("\"", "").trim();
		if(NumberUtils.isParsable(val))
		{
			headerDataMap.put(strs[0], Double.parseDouble(val));
			
		}else{
			headerDataMap.put(strs[0], val);
		}
	}
	
	
	private HashMap<String,Object> convertToHashMap(String[] headers, String line)
	{
		
		String[] datas = line.split(",");
		if(headers.length != datas.length)
			return null;
		
		HashMap<String,Object> map = new HashMap<String,Object>();
		//map.putAll(headerDataMap);
		
		String val;
		for(int idx = 0; idx < headers.length ; idx++)
		{
			val = datas[idx].replace("\"", "").trim();
			
			if(NumberUtils.isParsable(val))
			{	
				map.put(headers[idx], Double.parseDouble(val));
			}else{
				map.put(headers[idx], val);
				
			}
			
			
		}
		
		
		return map;
	}
}
