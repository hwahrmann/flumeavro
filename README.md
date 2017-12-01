RSA Netwitness Warehouse Connector is able to extract meta data captured on RSA Netwitness Log / Packet Decoders. This data is stored in Avro Files, which are being transfered to a NFS destination.

Apache Flume is consuming those Avro file.
This is a custom deserializer, which is able to process the events and store the data in e.g. ElasticSearch to be used with tools like Kibana
