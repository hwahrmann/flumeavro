package com.rsa.flume.serialization;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public final class Config {
	
	private final Logger logger = LoggerFactory.getLogger
		      (Config.class);
	
	private static Config singleton = null;
	private Boolean initialised = false;
	private Boolean ignoreRFC1918 = false;
	private String configFile = "/opt/flume/conf/FlumeAvroEventDeserializer.xml";
	private int kibanaVersion = 3;

	// HashMap with Time Correction Information for Device Types
	private static HashMap<String, Object> timeCorrection = new HashMap<String, Object>();
	
	// HashMap with Meta fields, whose length need to be truncated
	private static HashMap<String, Integer> truncateLength = new HashMap<String, Integer>();
	
	// Fields, which shall not be sent to elasticSearch
	private static HashMap<String, List<String>> excludedFields = new HashMap<String, List<String>>();

	// Fields, which shall be sent to elasticSearch
	private static HashMap<String, List<String>> includedFields = new HashMap<String, List<String>>();

	// Used to store Country Mapping between Netwitness and Kibana
	private static HashMap<String, String> countryMap = new HashMap<String, String>();
	
	private Config()
	{
		if (!initialised)
		{
			logger.info("Reading Configuration");
			ReadConfig();
			logger.info("Finished Reading Configuration");
		}
	}
	
	public static Config getinstance()
	{
		if (singleton == null)
		{
			singleton = new Config();
		}
		return singleton;
	}
	
	public Boolean IgnoreRFC1918() {
		return ignoreRFC1918;
	}

	public HashMap<String, Object> TimeCorrection()
	{
		return timeCorrection;
	}
	
	public HashMap<String, Integer> TruncateLength()
	{
		return truncateLength;
	}
	
	public List<String> ExcludedFields(String decoderName)
	{
		// If we don't find the decoderName in the list, we might have a condition for all decoders
		if (!excludedFields.containsKey(decoderName))
		{
			decoderName = "*";
		}
		
		ArrayList<String> fields = (ArrayList<String>)excludedFields.get(decoderName);
		return fields;
	}

	public List<String> IncludedFields(String decoderName)
	{
		// If we don't find the decoderName in the list, we might have a condition for all decoders
		if (!includedFields.containsKey(decoderName))
		{
			decoderName = "*";
		}
		
		ArrayList<String> fields = (ArrayList<String>)includedFields.get(decoderName);
		return fields;
	}

	public int KibanaVersion()
	{
		return kibanaVersion;
	}
	
	public HashMap<String, String> CountryMap()
	{
		return countryMap;
	}
	
	private void ReadConfig()
	  {
		  try {

				File fields = new File(configFile);
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(fields);

				XPath xPath = XPathFactory.newInstance().newXPath();
				
				// Get the list of fields, which should be excluded
				NodeList nodes = (NodeList)xPath.evaluate("/configuration/Exclude",
				        doc.getDocumentElement(), XPathConstants.NODESET);
		
				for (int i = 0; i < nodes.getLength(); i++) 
				{
					Element node = (Element) nodes.item(i);
					String[] decoderNames = node.getAttribute("Decoder").split(",");
					
					for (int k = 0; k < decoderNames.length; k++)
					{
						excludedFields.put(decoderNames[k], new ArrayList<String>());
					}
								
					NodeList fieldNodes = node.getElementsByTagName("Field");
					for (int j = 0; j < fieldNodes.getLength(); j++)
					{
						for (int k = 0; k < decoderNames.length; k++)
						{
							Element childNode = (Element)fieldNodes.item(j);
							ArrayList<String> fieldList = (ArrayList<String>)excludedFields.get(decoderNames[k]);
							fieldList.add(childNode.getFirstChild().getNodeValue().toString());
							excludedFields.put(decoderNames[k], fieldList);
						}
					}
				}

				// Get the list of fields, which should be included
				nodes = (NodeList)xPath.evaluate("/configuration/Include",
				        doc.getDocumentElement(), XPathConstants.NODESET);

				for (int i = 0; i < nodes.getLength(); i++) 
				{
					Element node = (Element) nodes.item(i);
					String[] decoderNames = node.getAttribute("Decoder").split(",");
					String includeAllFields = node.getAttribute("IncludeAllFields");
					
					// Decoder Names can be specified as pairs separated by commas
					for (int k = 0; k < decoderNames.length; k++)
					{
						includedFields.put(decoderNames[k], new ArrayList<String>());
						if (includeAllFields.equals("1"))
						{
							ArrayList<String> fieldList = (ArrayList<String>)includedFields.get(decoderNames[k]);
							fieldList.add("*");
							includedFields.put(decoderNames[k], fieldList);
						}
					}
					
					if (includeAllFields.equals("1"))
					{
						continue;
					}
					
					NodeList fieldNodes = node.getElementsByTagName("Field");
					for (int j = 0; j < fieldNodes.getLength(); j++)
					{
						for (int k = 0; k < decoderNames.length; k++)
						{
							Element childNode = (Element)fieldNodes.item(j);
							ArrayList<String> fieldList = (ArrayList<String>)includedFields.get(decoderNames[k]);
							fieldList.add(childNode.getFirstChild().getNodeValue().toString());
							includedFields.put(decoderNames[k], fieldList);
						}
					}
				}			
				
				// Get the list of devices, for which time correction shall be applied
				nodes = (NodeList)xPath.evaluate("/configuration/TimeCorrection/Device",
				        doc.getDocumentElement(), XPathConstants.NODESET);

				for (int i = 0; i < nodes.getLength(); i++) 
				{
					Node node = nodes.item(i);
					if (node.getNodeType() == Node.ELEMENT_NODE) {
						Element element = (Element) node;
						String name = element.getAttribute("name");
						int correction = Integer.parseInt(element.getAttribute("correction"));
						
						timeCorrection.put(name, correction);
					}
				} 
				
				// Get list of fields, which shall be Truncated 
				nodes = (NodeList)xPath.evaluate("/configuration/Truncate/Field",
				        doc.getDocumentElement(), XPathConstants.NODESET);

				for (int i = 0; i < nodes.getLength(); i++) 
				{
					Node node = nodes.item(i);
					if (node.getNodeType() == Node.ELEMENT_NODE) {
						Element element = (Element) node;
						String name = element.getAttribute("name");
						int length = Integer.parseInt(element.getAttribute("length"));
						truncateLength.put(name, length);
					}
				} 		
											
				// Check, if RFC 1918 addresses to be excluded
				String s1 = (String)xPath.evaluate("/configuration/IgnoreRFC1918/text()", doc.getDocumentElement());
				if (s1.equals("All"))
				{
					logger.info("Ignoring RFC 1918 addresses as per Configuration");
					ignoreRFC1918 = true;
				}
				
				kibanaVersion = Integer.parseInt(xPath.evaluate("/configuration/KibanaVersion/text()", doc.getDocumentElement())); 
				
				ReadCountryMap();
				
				initialised = true;
				
			} catch (Exception ex) {
				logger.error("Error reading Config: " + ex.getMessage());
			}
	  }
	
	private void ReadCountryMap()
	{
		String countryMapFile = "/opt/flume/conf/CountryMapping.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ";";
		int i = 0;

		try
		{
			br = new BufferedReader(new FileReader(countryMapFile));
			logger.info("Reading Netwitness to Kibana Country Mapping file.");
			while ((line = br.readLine()) != null)
			{
				i++;
				String[] countries = line.split(cvsSplitBy);
				countryMap.put(countries[0], countries[1]);
			}
		}
		catch (FileNotFoundException e) {
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally {
			if (br != null) {
				try 
				{
					br.close();
					logger.info("Found " + i + " mapped countries.");
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
