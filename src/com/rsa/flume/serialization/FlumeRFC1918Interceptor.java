package com.rsa.flume.serialization;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
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

public class FlumeRFC1918Interceptor implements
	Interceptor {

	private final Logger logger = LoggerFactory.getLogger
		      (FlumeRFC1918Interceptor.class);
	
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
		schema = EventSchema.getinstance().getSchema(event);
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
		} catch (IOException e) {
			return null;
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
			logger.info("Dropped Events because of RFC1918 addresses: " + removedEvents);
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
}

