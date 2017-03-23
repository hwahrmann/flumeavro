package com.rsa.flume.serialization;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;


import org.apache.flume.sink.elasticsearch.ContentBuilderUtil;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serialize SA Warehouse Connector Flume events into the same format LogStash uses</p>
 *
 * This can be used to send events to ElasticSearch and use clients such as
 * Kibana which expect Logstash formated indexes
 *
 * <pre>
 * {
 *    "@timestamp": "2010-12-21T21:48:33.309258Z",
 *    "@tags": [ "array", "of", "tags" ],
 *    "@type": "string",
 *    "@source": "source of the event, usually a URL."
 *    "@fields":{
 *       # a set of fields for this event
 *       "user": "jordan",
 *       "command": "shutdown -r":
 *     }
 *     "@message": "the original plain-text message"
 *   }
 * </pre>
 *
 * If the following headers are present, they will map to the above logstash
 * output as long as the logstash fields are not already present.</p>
 *
 * <pre>
 *  timestamp: long -> @timestamp:Date
 *  type: String -> @type: String
 *  source: String -> @source: String
 * </pre>
 *
 * @see https
 *      ://github.com/logstash/logstash/wiki/logstash%27s-internal-message-
 *      format
 */

/** 
 * Version History
 * ---------------
 * 
 * 29.03.2016 1.1 Added a switch to suppress RFC 1918 IP Addresses 
 * 22.03.2017 1.2 Added Kibana Version check, for geo_point creation
 * 23.03.2017 1.3 Added the ability to include / exclude fields based on the decodername
 * 
 */
public class FlumeAvroEventDeserializer  implements
	ElasticSearchEventSerializer {

	private final Logger logger = LoggerFactory.getLogger
	      (FlumeAvroEventDeserializer.class);
	
	private Config config = null;
	private Schema schema;
	private BinaryDecoder decoder = null;
	private String latSrc, latDst, longSrc, longDst = null;
	private String deviceType = null;
	private Long time = 0L;
	private Long eventTime = 0L;
	
	private String schemaHash = null;
	private Schema storedSchema;
	private String decoderName; 
	
	@Override
	  public XContentBuilder getContentBuilder(Event event) throws IOException {
		config = Config.getinstance();
	    XContentBuilder builder = jsonBuilder().startObject();
	    appendFields(builder, event);
	    builder.endObject(); 
	    return builder;
	  }

	  private void appendFields(XContentBuilder builder, Event event)
	      throws IOException {
		
		schema = getSchema(event);
		if (schema == null)
		{
			logger.error("Couldn't get a valid Schema. Abort processing of Event");
			return;
		}
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
	    decoder = DecoderFactory.get().binaryDecoder(event.getBody(), decoder);
	    GenericRecord datum = new GenericData.Record(schema);
	    
	    try
	    {
	    	datum = reader.read(datum, decoder);
	    }
	    catch (EOFException eof)
	    {
	    	return;
	    }
	    catch (Exception e) {
			logger.error("Exception reading event data: " + e.toString());
			return;
		}    
	        
	    // Reset the Coordinate fields
	    latSrc = latDst = longSrc = longDst = null;
	   
	    deviceType = "";
	    
	    // Reset the Time fields
	    time = eventTime = 0L;
	
	    decoderName = datum.get("ng_source").toString();
	    if (decoderName == null)
	    {
	    	decoderName = getDecoderNameFromFile(event.getHeaders().get("file"));
	    }
	        
	    builder.startObject("@fields");
	    for (Schema.Field field : schema.getFields()) {
	    	
	    	// Shall we ignore the field, based on configuration settings
	    	if (!includeOrExcludeField(field.name()))
	    	{
	    		continue;
	    	}
	    	    	
	    	Object value = datum.get(field.name());
	    	if (value != null)
	    	{
	    		// Check the presence of the Time fields
	    		// The TimeStamp will be set later
	    		if (field.name().equals("time")) {
	    			time = Long.parseLong(value.toString());
	    			continue;
	    		} else if (field.name().equals("event_time")) {
	    			eventTime = Long.parseLong(value.toString());
	    			continue;
	    		}
	    		
	    		// check for the presence of Geo-IP
	    		if (field.name().startsWith("latdec_src"))
	    		{
	    			latSrc = value.toString();  
	    			continue;
	    		} else if (field.name().startsWith("latdec_dst"))
	    		{
	    			latDst = value.toString();
	    			continue;
	    		} else if (field.name().startsWith("longdec_src"))
	    		{
	    			longSrc = value.toString();
	    			continue;
	    		} else if (field.name().startsWith("longdec_dst"))
	    		{
	    			longDst = value.toString();
	    			continue;
	    		}
	    		
	    		// Store devic_type, so that we can use it later for event_time correction
	    		if (field.name().equals("device_type"))
	    		{
	    			deviceType = value.toString();
	    		}
	    		
	    		String fieldValue = value.toString();
	    		
	    		// Do we need a truncation to avoid problems with lengthy fields
	    		if (config.TruncateLength().containsKey(field.name()))
		    	{
		    		fieldValue = fieldValue.substring(0, Math.min(fieldValue.length(), config.TruncateLength().get(field.name())));
		    	}
	    		
	    		if (value instanceof Boolean)
	    		{
	    			if (fieldValue == "T")
	    			{
	    				fieldValue = "true";
	    			}
	    			else if (fieldValue == "F")
	    			{
	    				fieldValue = "false";
	    			}
	    		}
	    		
	    		byte[] val = fieldValue.getBytes(charset);
		    	ContentBuilderUtil.appendField(builder, field.name(), val);
	    	}
	    }
	        
	    // Check if we got valid GEO IP Info
	    if (latSrc != null && longSrc != null)
	    {
	    	if  (config.KibanaVersion() > 3)
	    	{
	    		builder.startObject("location_src");
	    		builder.field("lat", Double.parseDouble(latSrc));
	    		builder.field("lon", Double.parseDouble(longSrc));
	    		builder.endObject();
	    	}
	    	else
	    	{
	    		builder.field("location_src", Lists.newArrayList(Double.parseDouble(longSrc), Double.parseDouble(latSrc)));	
	    	}
	    }
	    
	    if (latDst != null && longDst != null)
	    {
	    	if  (config.KibanaVersion() > 3)
	    	{
	    		builder.startObject("location_dst");
	    		builder.field("lat", Double.parseDouble(latDst));
	    		builder.field("lon", Double.parseDouble(longDst));
	    		builder.endObject();
	    	}
	    	else
	    	{
	    		builder.field("location_dst", Lists.newArrayList(Double.parseDouble(longDst), Double.parseDouble(latDst)));
	    	}
	    }
	    
        builder.endObject();   

        // Now let's set the TimeStamp
        // First we user event_time, if present, to get the "real" time, when the event occured
        // Then we use the Time, which was present in the event
        // If not, we use the Time of the Flume event
        Long timestamp = 0L;
        if (eventTime > 0L) {
        	int correction = 0;
        	if (config.TimeCorrection().containsKey(deviceType))
        	{
        		correction = (int)config.TimeCorrection().get(deviceType);
        	}       	
        	timestamp = eventTime + ((long)correction * 3600L); // The Real Event Time and adjust by adding the time correction (negative time correction to be denoted in the xml)
        } else if (time > 0L) {
        	timestamp = time;      // The Capture time of the event
        } else {
        	timestamp = (Long)datum.get("time");
        }
        
        // Convert to milliseconds first
    	timestamp = timestamp * 1000L;
    	Date date = new Date(timestamp);
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    	sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    	String formattedDate = sdf.format(date);
    	builder.field("@timestamp", formattedDate);
   	
	    // Set the Decoder Name as Source
		ContentBuilderUtil.appendField(builder, "@source", decoderName.getBytes(charset));
	  }
  

	  private boolean includeOrExcludeField(String field)
	  {
		// Do we need to ignore the field?
		if (config.ExcludedFields(decoderName).contains(field))
	    {
			return false;
	    }
	    	
	    // Do we need to include the field?
	    if (config.IncludedFields(decoderName).contains(field) || config.IncludedFields(decoderName).contains("*"))
	    {
	    	return true;
	    }
	    return false;
	  }
	  
	  /**
	   * In case, we didn't get the decoder name from the "ng_source" field, we will
	   * try to extract it from the filename.
	   * 
	   * @param file
	   * @return
	   */
	  private String getDecoderNameFromFile(String file)
	  {
	      // Get the name of the Decoder out of the filename
		  // Sample file names:
		  // sessions-warehouseconnector-eb-rng-aptdec1-es-34835-1425401857360-TS2015-3-3-14-23TE.avro
		  // sessions-warehouseconnector-eb-gb-aptlog1-elasticsearch-1033-1424936607584-TS2015-2-26-6-43TE.avro
		  // The decoder name would be: eb-rng-aptdec1 or eb-gb-aptlog1 
		  return file.substring(file.indexOf("sessions-warehouseconnector-") + 28,file.lastIndexOf("-", file.lastIndexOf("-", file.lastIndexOf("-", file.lastIndexOf("-TS") - 1) - 1) - 1 ));
	  }
	  
	  
	  /**
	   * Gets the Schema information out of the Event.
	   * If it is LITERAL, then parse the schema
	   * If it is HASH, then compare if the hash changed and reread the schema from the file
	   * 
	   * @param event
	   * @return
	  */
	  private Schema getSchema(Event event)
	  {
		  Schema schema = null;
		  Map<String, String> headers = Maps.newHashMap(event.getHeaders());
		  
		  if (headers.containsKey("flume.avro.schema.literal"))
		  {
			  schema = new Schema.Parser().parse(headers.get("flume.avro.schema.literal"));
		  }
		  else if (headers.containsKey("flume.avro.schema.hash"))
		  {
			  String hash = headers.get("flume.avro.schema.hash");
			  if (hash != schemaHash)
			  {
				  schemaHash = hash;
				  schema = readSchemaString(headers.get("file"));
				  // In rare cases it happens that Flume renames the file, while we were trying to get the schema
				  // try to read the schema up to 10 times
				  int count = 0;
				  while (schema == null && count < 10)
				  {
					  try {
						Thread.sleep(100);
					  } catch (InterruptedException e) {
							
					  }
					  schema = readSchemaString(headers.get("file"));
					  count++;
				  }
				  
				  storedSchema = schema;
			  }
			  else
			  {
				  schema = storedSchema;
			  }
		  }
		  return schema;
	  }
	  
	  private Schema readSchemaString(String file)
	  {
		  // See if file still exists or if it had been renamed already by Flume
		  File f = new File(file);
		  if (!f.exists())
		  {
			  file = file + ".COMPLETED";
		  }
		  	  
		  logger.debug("Using file " + file);
		  	  
		  FileReader<?> fileReader = null;
		  Schema schema = null;
		  try
		    {
			  GenericDatumReader<?> reader = new GenericDatumReader<Object>();
			  fileReader = DataFileReader.openReader(new File(file), reader);
			  
		      schema = fileReader.getSchema();
		    } catch (IOException e) {
		    	logger.error("IOException getting schema: " + e.getMessage());
		    	return null;
			} catch (NullPointerException e1) {
				logger.error("NullPointer Exception getting schema: " + e1.getMessage());
				return null;
			} finally {
		      try {
				fileReader.close();
		      } catch (IOException e) {
		    	  return schema;
		      } catch (NullPointerException e1) {
					return schema;
		      }
		    }
		  return schema;
	  }
	  
	  @Override
	  public void configure(Context context) {
	    // NO-OP...
	  }

	  @Override
	  public void configure(ComponentConfiguration conf) {
	    // NO-OP...
	  }
}
