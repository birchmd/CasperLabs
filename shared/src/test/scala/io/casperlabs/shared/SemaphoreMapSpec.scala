package io.casperlabs.shared
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntBinaryOperator
import org.scalatest.{FlatSpec, Inspectors, Matchers}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

class SemaphoreMapSpec extends FlatSpec with Matchers with Inspectors {

  "SemaphoreMap" should "only allow concurrency up to the capacity per key" in {
    class Counter() {
      val current = new AtomicInteger(0)
      val max     = new AtomicInteger(0)
    }

    val counterMap = TrieMap.empty[Int, Counter]
    val capacity   = 2

    val test = for {
      semaphoreMap <- SemaphoreMap[Task, Int](capacity)
      tasks = List.range(0, 99).map { i =>
        val k = i % 10
        semaphoreMap.withPermit(k) {
          for {
            counter <- Task.delay {
                        val counter = counterMap.getOrElseUpdate(k, new Counter())
                        val curr    = counter.current.incrementAndGet()
                        counter.max.accumulateAndGet(curr, new IntBinaryOperator {
                          def applyAsInt(left: Int, right: Int) = math.max(left, right)
                        })
                        counter
                      }
            _ <- Task.sleep(50.millis)
            _ <- Task.delay {
                  counter.current.decrementAndGet()
                }
          } yield ()
        }
      }
      _ <- Task.gatherUnordered(tasks)
    } yield {
      forAll(counterMap.values) { c =>
        c.current.get shouldBe 0
        c.max.get should be > 0
        c.max.get should be <= capacity
      }
    }

    test.runSyncUnsafe(10.seconds)
  }
}
