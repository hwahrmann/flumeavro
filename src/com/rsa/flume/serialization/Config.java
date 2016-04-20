package com.rsa.flume.serialization;

import java.io.File;
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

	// HashMap with Time Correction Information for Device Types
	private static HashMap<String, Object> timeCorrection = new HashMap<String, Object>();
	
	// HashMap with Meta fields, whose length need to be truncated
	private static HashMap<String, Integer> truncateLength = new HashMap<String, Integer>();
	
	// Fields, which shall not be sent to elasticSearch
	private static List<String> excludedFields = new ArrayList<String>();
	
	private Config()
	{
		if (!initialised)
		{
			logger.info("Reading Configuration");
			ReadConfig();
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
	
	public List<String> ExcludedFields()
	{
		return excludedFields;
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
				NodeList nodes = (NodeList)xPath.evaluate("/configuration/Exclude/Field",
				        doc.getDocumentElement(), XPathConstants.NODESET);

				for (int i = 0; i < nodes.getLength(); i++) 
				{
					Node node = nodes.item(i);
					if (node.getNodeType() == Node.ELEMENT_NODE) {
						Element element = (Element) node;
						String name = element.getAttribute("name");
						excludedFields.add(name);
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
				
				initialised = true;
				
			} catch (Exception ex) {
				logger.error("Error reading Config: " + ex.getMessage());
			}
	  }
}
