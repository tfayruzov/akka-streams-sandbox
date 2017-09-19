package com.tfayruzov.akka

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object HttpClientTest extends App {

  implicit val system = ActorSystem("akka-http-test")
  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val host = "localhost"
  val port = 8081

  import Streams._

  val server = new TestWebServer {

    def captureResult(start: Long, f: Future[_], name: String) = {
      f.onComplete {
        case Success(_) =>
          val latency =
            Duration(System.nanoTime - start, TimeUnit.NANOSECONDS).toMillis
          println(
            s"$name completed. Took: $latency ms. Avg: ${latency / nRequests} ms.")
        case Failure(ex) =>
          println(s"$name failed: ${ex.getMessage}"); ex.printStackTrace
      }
    }

    override def postHttpBinding(binding: Http.ServerBinding): Unit = {
      super.postHttpBinding(binding)
      // warmup run followed by actual run
      //oneRun.flatMap(_ => oneRun)

      oneInstrRun.flatMap(_ => oneInstrRun)
    }

    def oneRun = {
      val start1 = System.nanoTime
      val res1 = singleRequestRun("singleRequest")
      captureResult(start1, res1, "singleRequest")

      val res2 = res1.flatMap(_ => {
        val start = System.nanoTime
        val res = cachedPoolRun("cachedPool")
        captureResult(start, res, "cachedPool")
        res
      })

      val res3 = res2.flatMap(_ => {
        val start = System.nanoTime
        val res = queuedPoolRun("queuedPool")
        captureResult(start, res, "queuedPool")
        res
      })

      res3.onComplete {
        case Success(_) => println("Test completed")
        case Failure(_) => println("Test failed")
      }

      res3
    }

    def oneInstrRun = {
      val start1 = System.nanoTime
      val res1 = instrSingleRequestRun("singleRequest")
      captureResult(start1, res1, "singleRequest")

      val res2 = res1.flatMap(_ => {
        val start = System.nanoTime
        val res = instrCachedPoolRun("cachedPool")
        captureResult(start, res, "cachedPool")
        res
      })

      val res3 = res2.flatMap(_ => {
        val start = System.nanoTime
        val res = instrQueuedPoolRun("queuedPool")
        captureResult(start, res, "queuedPool")
        res
      })

      res3.onComplete {
        case Success(_) => println("Test completed")
        case Failure(_) => println("Test failed")
      }

      res3
    }
  }

  println("Starting Server ...")

  // this is a blocking call
  server.startServer(host, port)

  object Streams {

    val nRequests = 100

    val mapAsyncFactor = 16

    val superPool = Http().superPool[Int]()

    val cachedPool =
      Http().cachedHostConnectionPool[Int](host, port)

    val poolClientFlow =
      Http().cachedHostConnectionPool[Promise[HttpResponse]](host, port)

    val queuedPool =
      Source
        .queue[(HttpRequest, Promise[HttpResponse])](100,
                                                     OverflowStrategy.dropNew)
        .via(poolClientFlow)
        .toMat(Sink.foreach({
          case ((Success(resp), p)) => p.success(resp)
          case ((Failure(e), p))    => p.failure(e)
        }))(Keep.left)
        .run()

    val asyncRequestFlow = Flow[Int].mapAsync(mapAsyncFactor)(
      i =>
        Http()
          .singleRequest(HttpRequest(uri = s"http://$host:$port/?id=$i"))
          .flatMap(response =>
            response.entity
              .toStrict(timeout = 5.seconds)
              .map(_.data.utf8String.toInt)))

    def queueRequest(request: HttpRequest): Future[HttpResponse] = {
      val responsePromise = Promise[HttpResponse]()
      queuedPool.offer(request -> responsePromise).flatMap {
        case QueueOfferResult.Enqueued => responsePromise.future
        case QueueOfferResult.Dropped =>
          Future.failed(
            new RuntimeException("Queue overflowed. Try again later."))
        case QueueOfferResult.Failure(ex) => Future.failed(ex)
        case QueueOfferResult.QueueClosed =>
          Future.failed(new RuntimeException(
            "Queue was closed (pool shut down) while running the request. Try again later."))
      }
    }

    val queueRequestFlow = Flow[Int]
      .map(i => HttpRequest(uri = s"http://$host:$port/?id=$i"))
      .mapAsync(mapAsyncFactor)(
        request =>
          queueRequest(request).flatMap(
            response =>
              response.entity
                .toStrict(timeout = 5.seconds)
                .map(_.data.utf8String.toInt)))

    val streamRequestFlow = Flow[Int]
      .map(i => HttpRequest(uri = s"http://$host:$port/?id=$i") -> i)
      .via(cachedPool)
      .mapAsync(mapAsyncFactor) {
        case (Success(resp), _) =>
          resp.entity.toStrict(timeout = 5.seconds).map(_.data.utf8String.toInt)
        case (Failure(ex), _) =>
          println(ex.getMessage); ex.printStackTrace(); throw ex
      }

    def singleRequestRun(name: String) = {
      println(name)
      Source(1 to nRequests)
        .via(asyncRequestFlow)
        .runWith(Sink.ignore)
    }

    def cachedPoolRun(name: String) = {
      println(name)
      Source(1 to nRequests)
        .via(streamRequestFlow)
        //.mapAsync(1)(x => { x.foreach(println); x })
        .runWith(Sink.ignore)
    }

    def queuedPoolRun(name: String) = {
      println(name)
      Source(1 to nRequests)
        .via(queueRequestFlow)
        .runWith(Sink.ignore)
    }

    import InstrumentedFlow._

    val monitorF = (instr: InstrumentedData[Int]) => {
//      println(
//        s"Processing ${instr.metricName}. Element ${instr.data} took ${FiniteDuration(
//          instr.end.get - instr.start,
//          TimeUnit.NANOSECONDS).toMillis} ms")
    }

    def instrSingleRequestRun(name: String) = {
      println(name)
      Source(1 to nRequests)
        .via(instrumentedFlow(name, asyncRequestFlow, monitorF))
        .runWith(Sink.ignore)
    }

    def instrCachedPoolRun(name: String) = {
      println(name)
      Source(1 to nRequests)
        .via(instrumentedFlow(name, streamRequestFlow, monitorF))
        .runWith(Sink.ignore)
    }

    def instrQueuedPoolRun(name: String) = {
      println(name)
      Source(1 to nRequests)
        .via(instrumentedFlow(name, queueRequestFlow, monitorF))
        .runWith(Sink.ignore)
    }

  }

}
