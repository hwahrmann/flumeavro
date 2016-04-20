package com.rsa.flume.serialization;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.avro.Schema;
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
// The above has been commented because of Flume errors
//import com.frontier45.flume.sink.elasticsearch2.ContentBuilderUtil;
//import com.frontier45.flume.sink.elasticsearch2.ElasticSearchEventSerializer;
import com.google.common.collect.Lists;

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
		
	@Override
	  public XContentBuilder getContentBuilder(Event event) throws IOException {
		config = Config.getinstance();
	    XContentBuilder builder = jsonBuilder().startObject();
	    appendFields(builder, event);   
	    return builder;
	  }

	  private void appendFields(XContentBuilder builder, Event event)
	      throws IOException {
		
		schema = EventSchema.getinstance().getSchema(event);
		if (schema == null)
		{
			logger.error("Couldn't get a valid Schema. Abort processing of Event");
			return;
		}
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
	    decoder = DecoderFactory.get().binaryDecoder(event.getBody(), decoder);
	    GenericRecord datum = new GenericData.Record(schema);
	    datum = reader.read(datum, decoder);
	        
	    // Reset the Coordinate fields
	    latSrc = latDst = longSrc = longDst = null;
	   
	    deviceType = "";
	    
	    // Reset the Time fields
	    time = eventTime = 0L;
	    
	    builder.startObject("@fields");
	    for (Schema.Field field : schema.getFields()) {
	    	// Do we need to ignore the field?
	    	if (config.ExcludedFields().contains(field.name()))
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
	    		   		
	    		byte[] val = fieldValue.getBytes(charset);
		    	ContentBuilderUtil.appendField(builder, field.name(), val);
	    	}
	    }
	        
	    // Check if we got valid GEO IP Info
	    if (latSrc != null && longSrc != null)
	    {
	    	builder.field("location_src", Lists.newArrayList(Double.parseDouble(longSrc), Double.parseDouble(latSrc)));	
	    }
	    
	    if (latDst != null && longDst != null)
	    {
	    	builder.field("location_dst", Lists.newArrayList(Double.parseDouble(longDst), Double.parseDouble(latDst)));
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
		ContentBuilderUtil.appendField(builder, "@source", EventSchema.getinstance().DecoderName().getBytes(charset));
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
