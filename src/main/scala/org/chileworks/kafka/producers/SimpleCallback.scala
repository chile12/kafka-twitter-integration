package org.chileworks.kafka.producers

import org.apache.kafka.clients.producer.Callback

/* just an example callback function */
class SimpleCallback extends Callback{

  import org.apache.kafka.clients.producer.RecordMetadata

  def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) System.out.println(exception.getMessage)
  }
}
