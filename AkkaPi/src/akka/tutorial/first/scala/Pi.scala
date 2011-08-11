package akka.tutorial.first.scala

import akka.actor.{Actor, PoisonPill}
import akka.routing.{Routing, CyclicIterator}
import Routing._
import akka.dispatch.Dispatchers
import java.util.concurrent.CountDownLatch

/** Application to calculate pi using a collection of actors working
  * concurrently.
  */
object Pi extends App
{
  // Main code of app.
  calculate(nrOfWorkers = 4, nrOfElements = 10000, nrOfMessages = 10000)

  def calculate(nrOfWorkers: Int, nrOfElements: Int, nrOfMessages: Int) {
    val latch = new CountDownLatch(1)
    val master =
      Actor.actorOf(
        new Master(nrOfWorkers, nrOfMessages, nrOfElements, latch)).start()
    master ! Calculate
    latch.await()
  }

  // Messages used.
  sealed trait PiMessage
  case object Calculate extends PiMessage
  case class Work(start: Int, nrOfElements: Int) extends PiMessage
  case class Result(value: Double) extends PiMessage


  /** Worker actor to calculate pieces of the result.
    */
  class Worker extends Actor {
    def receive = {
    case Work(start, nrOfElements) =>
    self reply Result(calculatePiFor(start, nrOfElements))
  }

  private def calculatePiFor(start: Int, nrOfElements: Int): Double = {
    var acc = 0.0
    for (i <- start until (start + nrOfElements))
      acc += 4.0 * (1 - (i % 2) *2 ) / (2 * i + 1)
    acc
    }
  }

  /** Master actor to coordinate the workers.
    */
  class Master (
    nrOfWorkers: Int, nrOfMessages: Int, nrOfElements: Int,
    latch: CountDownLatch)
    extends Actor
  {
    var pi: Double = _
    var nrOfResults: Int = _
    var start: Long = _

    val workers = Vector.fill(nrOfWorkers)(Actor.actorOf[Worker].start())
    val router = Routing.loadBalancerActor(CyclicIterator(workers)).start()

    def receive = {

      case Calculate =>
        for (i <- 0 until nrOfMessages)
          router ! Work(i * nrOfElements, nrOfElements)
      router ! PoisonPill

      case Result(value) =>
        pi += value
        nrOfResults += 1
        if (nrOfResults == nrOfMessages)
          self.stop()
    }

    override def preStart() {
      start = System.currentTimeMillis
    }

    override def postStop() {
      println(
        "\n\tPi estimate: \t\t%s\n\tCalculation time: \t%s millis"
        .format(pi, System.currentTimeMillis - start))
      latch.countDown()
    }
  }
}
