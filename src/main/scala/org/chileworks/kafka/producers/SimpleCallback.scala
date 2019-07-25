package org.chileworks.kafka.producers

import org.apache.kafka.clients.producer.Callback

class SimpleCallback extends Callback{

  import org.apache.kafka.clients.producer.RecordMetadata

  //TODO just for initial callback
  def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception == null) System.out.println("Message with offset %d acknowledged by partition %d\n", metadata.offset, metadata.partition)
    else System.out.println(exception.getMessage)
  }
}
