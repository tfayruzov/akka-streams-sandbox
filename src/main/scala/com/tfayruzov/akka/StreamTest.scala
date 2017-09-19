package com.tfayruzov.akka

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object StreamTest extends App {
  import akka.actor.ActorSystem
  import akka.stream._
  import akka.stream.scaladsl._

  //  val decider: Supervision.Decider = {
  //    case _: ArithmeticException => Supervision.Resume
  //    case _                      => Supervision.Stop
  //  }

  implicit val system = ActorSystem("akka-http-test")
  implicit val materializer = ActorMaterializer()
  //ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  import system.dispatcher

  import InstrumentedFlow._

  val clientFlow = Http().superPool[Int]()

  val flowBasedPool =
    Http().cachedHostConnectionPool[Int]("localhost", 8081)

  /*
 Ways to run a 3rd party request:

 1. SingleRequest (future needs to be wrapped with mapAsync)
 2. cachedHostConnectionPool ()
 3.
   */

  // Example usage
  val processing = Flow[Int]
    .map(i => {
      HttpRequest(uri = s"http://localhost:8081?i=$i") -> 500 // 500 is random here, supposed to somehow use it on app layer to reconcile async responses.
    })
    .via(flowBasedPool)
    //.via(clientFlow)
    .mapAsyncUnordered(8) {
      case (Success(resp), _) =>
        resp.entity.toStrict(timeout = 5.seconds).map(_.data.utf8String.toInt)
      case (Failure(ex), _) =>
        println(ex.getMessage); ex.printStackTrace(); throw ex
    }
  // QUESTION: Timeout/failure handling? Options: single step faiure, recovery, supervision strategies?

  val monitoringF = (instr: InstrumentedData[Int]) => {
    println(
      s"Processing ${instr.data} took ${(instr.end.get - instr.start) / 1000} ")
  }

  println("Starting source ...")

  val start = System.nanoTime

  val res =
    Source(1 to 100)
      .via(instrumentedFlow("counter", processing, monitoringF))
      .runWith(Sink.ignore)

  res.onComplete {
    case Success(_) =>
      println(
        s"Stream completed. Took: ${Duration(System.nanoTime - start, TimeUnit.NANOSECONDS).toMillis} ms")
    case Failure(ex) =>
      println(s"Stream failed: ${ex.getMessage}"); ex.printStackTrace
  }

  println("Starting Server ...")

  val server = new TestWebServer {}
  // this is test server
  server.startServer("localhost", 8081)

}

// alternative: custom GraphStage for monitoring.
