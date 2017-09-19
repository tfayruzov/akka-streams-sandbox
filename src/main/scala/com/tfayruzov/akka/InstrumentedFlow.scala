package com.tfayruzov.akka

import akka.NotUsed
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Zip}

object InstrumentedFlow {

  // The envelope for data that adds instrumentation. It is an implementation detail of the instrumentation graph,
  // but needs to be used in the monitoring function.
  case class InstrumentedData[T](metricName: String,
                                 start: Long,
                                 end: Option[Long],
                                 data: T)

  /*

                                      +---------------------------+
             +--------------------+   |                           |   +---------------------+
       in ~> |startInstrumentation| - +                           + ~>|endInstrumentation   | ~> out
             +--------------------+   |                           |   |    - monitorFunction|
                                      |      +-----+    +------+  |   +---------------|-----+
                                      + - ~> |strip| ~> |target| -+                   + ~> outside
                                             +-----+    +------+

      startInstrumentation - wraps the `in` payload in the monitoring envelope
      strip - extracts the payload form the monitoring envelope
      target - the Flow that we want to instrument
      endInstrumentation - calculate monitoring values using startInstrumentation values (for latency measurements) and
                           side-effecting call to monitoring subsystem


   This flow can wrap any flow and user can control the communication with monitoring subsystem using monitorFunction.

   It does not leverage potential parallelism in `target` Flow because Zip stage will backpressure Broadcast to merge
   metric with the response. On the other hand, it guarantees correct merges for start/end instrumentations.

   */

  def instrumentedFlow[T, U](metricName: String,
                             target: Flow[T, U, _],
                             monitorFunction: InstrumentedData[U] => Unit)
    : Graph[FlowShape[T, U], NotUsed] = {

    GraphDSL.create() { implicit builder =>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[InstrumentedData[T]](2))
      val strip = builder.add(Flow[InstrumentedData[T]].map(_.data))
      val zip = builder.add(Zip[InstrumentedData[T], U])
      val startInstrumentation =
        builder.add(Flow[T].map(data =>
          InstrumentedData(metricName, System.nanoTime, None, data)))
      val endInstrumentation =
        builder.add(Flow[(InstrumentedData[T], U)].map(tuple => {
          val instrStart = tuple._1
          val resultData = tuple._2
          val instrEnd = InstrumentedData(instrStart.metricName,
                                          instrStart.start,
                                          Some(System.nanoTime),
                                          resultData)
          // generally you're not supposed to know the data types here, this is just for debugging.
          //println(s"${resultData.toString.toInt - instrStart.data.toString.toInt}")
          monitorFunction(instrEnd)
          resultData
        }))

      // format: off
      startInstrumentation ~> broadcast                    ~> zip.in0
                              broadcast ~> strip ~> target ~> zip.in1
      zip.out ~> endInstrumentation
      // format: on

      FlowShape(startInstrumentation.in, endInstrumentation.out)
    }
  }

}
