package com.tfayruzov.akka

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.{HttpApp, Route}

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Random

// Web server that completes requests with random or fixed latencies.
abstract class TestWebServer extends HttpApp {

  val maxLatencyMs = 100

  // schedules a response to take either random amount of time up to maxLatencyMs, or a fixed amount of time.
  def scheduleCompletion(p: Promise[HttpResponse],
                         id: String,
                         delay: Option[FiniteDuration] = None) = {
    implicit val ec = systemReference.get.dispatcher
    systemReference.get.scheduler.scheduleOnce(
      delay.getOrElse(Random.nextInt(maxLatencyMs).millis),
      new Runnable {
        override def run() =
          p.success(
            HttpResponse(
              entity = HttpEntity(ContentTypes.`application/json`, id)))
      }
    )

  }

  override def routes: Route =
    get {
      parameter('id) { id => // localhost:8081/?id=x
        complete {
          val p = Promise[HttpResponse]
          scheduleCompletion(
            p,
            id,
            Some(FiniteDuration(maxLatencyMs, TimeUnit.MILLISECONDS)))
          p.future
        }
      }
    }
}
