package com.tfayruzov.akka

import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.BidiFlow
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/*
                         BidiFlow
             +---------------------------------+  instrumentedFlowIn
   in ------>| capture start time;             |------------------>|
             | pass through the original value;|                   |
             |                                 |                   | original flow
             | capture completion time;        |                   |
   out <-----| send monitoring signal;         |<------------------v
             | pass through the original value;|  instrumentedFlowOut
             +---------------------------------+

 THe Bidi flow that wraps the target Flow and provides side-effecting connection to the monitoring system via callback.
 */

class InstrumentationStage[In, Out](inKeyBuilder: In => String,
                                    outKeyBuilder: Out => String,
                                    monitor: (In, Out, FiniteDuration) => Unit)
    extends GraphStage[BidiShape[In, In, Out, Out]] {

  val in = Inlet[In]("Flow input")
  val out = Outlet[Out]("Flow output")

  val instrumentedFlowIn = Outlet[In]("Instrumented flow input")
  val instrumentedFlowOut = Inlet[Out]("Instrumented flow output")

  case class StartInstrumentation(start: Long, data: In)

  override def shape =
    BidiShape.of(in, instrumentedFlowIn, instrumentedFlowOut, out)

  override def createLogic(inheritedAttributes: Attributes) =
    new GraphStageLogic(shape) {
      val instrMap = mutable.Map.empty[String, StartInstrumentation]

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val element = grab(in)
            instrMap.put(inKeyBuilder(element),
                         StartInstrumentation(System.nanoTime, element))
            push(instrumentedFlowIn, element)
          }
        }
      )

      setHandler(instrumentedFlowIn, new OutHandler {
        override def onPull(): Unit = pull(in)
      })

      setHandler(
        instrumentedFlowOut,
        new InHandler {
          override def onPush(): Unit = {
            val element = grab(instrumentedFlowOut)
            val key = outKeyBuilder(element)
            val startInstr = instrMap(key)
            instrMap.remove(key)
            val end = System.nanoTime
            monitor(
              startInstr.data,
              element,
              FiniteDuration(end - startInstr.start, TimeUnit.NANOSECONDS))
            push(out, element)
          }
        }
      )

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(instrumentedFlowOut)
      })
    }

}

object InstrumentationStage {
  def apply[In, Out](inKeyBuilder: In => String,
                     outKeyBuilder: Out => String,
                     monitor: (In, Out, FiniteDuration) => Unit) =
    BidiFlow.fromGraph(
      new InstrumentationStage(inKeyBuilder, outKeyBuilder, monitor))
}
