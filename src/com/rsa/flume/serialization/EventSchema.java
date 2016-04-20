package com.rsa.flume.serialization;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public final class EventSchema {
	
	private final Logger logger = LoggerFactory.getLogger
		      (EventSchema.class);

	private static EventSchema singleton = null;
	private String schemaHash = null;
	private Schema storedSchema;
	private String decoderName; 
	
	private EventSchema()
	{
		schemaHash = null;
	}
	
	public String DecoderName() {
		return decoderName;
	}
	
	public static EventSchema getinstance()
	{
		if (singleton == null)
		{
			singleton = new EventSchema();
		}
		return singleton;
	}
	
	
	  /**
	   * Gets the Schema information out of the Event.
	   * If it is LITERAL, then parse the schema
	   * If it is HASH, then compare if the hash changed and reread the schema from the file
	   * 
	   * @param event
	   * @return
	  */
	  public Schema getSchema(Event event)
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
		  
	      // Get the name of the Decoder out of the filename
		  // Sample file names:
		  // sessions-warehouseconnector-eb-rng-aptdec1-es-34835-1425401857360-TS2015-3-3-14-23TE.avro
		  // sessions-warehouseconnector-eb-gb-aptlog1-elasticsearch-1033-1424936607584-TS2015-2-26-6-43TE.avro
		  // The decoder name would be: eb-rng-aptdec1 or eb-gb-aptlog1 
		  decoderName = file.substring(file.indexOf("sessions-warehouseconnector-") + 28,file.lastIndexOf("-", file.lastIndexOf("-", file.lastIndexOf("-", file.lastIndexOf("-TS") - 1) - 1) - 1 ));
		  
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

}
