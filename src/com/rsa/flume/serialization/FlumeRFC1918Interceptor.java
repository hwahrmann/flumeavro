package com.rsa.flume.serialization;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class FlumeRFC1918Interceptor implements
	Interceptor {

	private final Logger logger = LoggerFactory.getLogger
		      (FlumeRFC1918Interceptor.class);

	
	private String schemaHash = null;
	private Schema storedSchema;
	
	private Schema schema;
	private BinaryDecoder decoder = null;
	private Config config = null;
	
	private FlumeRFC1918Interceptor(Context ctx) {
		config = Config.getinstance();
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void initialize() {
	}

	@Override
	public Event intercept(Event event) {
		schema = getSchema(event);
		if (schema == null)
		{
			logger.error("Couldn't get a valid Schema. Abort processing of Event");
			return null;
		}
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
		decoder = DecoderFactory.get().binaryDecoder(event.getBody(), decoder);
	    GenericRecord datum = new GenericData.Record(schema);
	    try {
			datum = reader.read(datum, decoder);
		} 
	    catch (EOFException eof)
	    {
	    	return event;
	    }
	    catch (Exception e) {
			return event;
		}
		
	    Object medium = datum.get("medium");
	    if (medium != null && (int)medium == 32)
	    {
	    	return event;
	    }
	    
	    String ipSrc = null;
	    String ipDst = null;
	    
	    try
	    {
		    ipSrc = datum.get("ip_src").toString();
		    ipDst = datum.get("ip_dst").toString();	    	
	    } catch (Exception ex) {
	    }
		
	    if (ipSrc == null && ipDst == null)
	    {
	    	return event;
	    }
	    
		InetAddress srcip = null;
	    InetAddress dstip = null;
		try {
			srcip = InetAddress.getByName(ipSrc);
			dstip = InetAddress.getByName(ipDst);
		} catch (UnknownHostException e) {
		}
			
	    if (srcip.isSiteLocalAddress() || dstip.isSiteLocalAddress())
	    {
	      	return null;
	    }
	    return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		if (!config.IgnoreRFC1918())
		{
			return events;
		}
		
		int removedEvents = 0;
		for (Iterator<Event> iterator = events.iterator(); iterator.hasNext(); ) {
            Event event =  intercept(iterator.next());
            if(event == null) {
                // remove the event
                iterator.remove();
                removedEvents++;
            }
        }
		
		if (removedEvents > 0)
		{
			logger.debug("Dropped Events: " + removedEvents);
		}
		
        return events;
	}
	
	public static class FlumeRFC1918InterceptorBuilder implements Interceptor.Builder {

        private Context ctx;

        @Override
        public Interceptor build() {
            return new FlumeRFC1918Interceptor(ctx);
        }

        @Override
        public void configure(Context context) {
        this.ctx = context;
        }
	}
	
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

}

