package com.daml.platform.store.state

import com.daml.caching.CaffeineCache.SimpleCaffeineCache
import com.daml.caching.{Cache => DamlCache}
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Random

class StateCacheSpec extends AnyFlatSpec with Matchers with MockitoSugar with Eventually {
  behavior of "async cache loading"

  private val stateCacheBuilder = (provided: DamlCache[String, String]) =>
    new StateCache[String, String] {
      override protected implicit def ec: ExecutionContext =
        scala.concurrent.ExecutionContext.global

      override def cache: DamlCache[String, String] = provided
    }

  it should "always store the latest key update in face of conflicting pending updates" in {
    val caffeineCache: Cache[String, String] = Caffeine
      .newBuilder()
      .maximumSize(3)
      .build()
    val cache = new SimpleCaffeineCache[String, String](caffeineCache)
    val updates = (1 to 10000).map(idx => (idx.toLong, "same-key", Promise[String]))
    val (indices, _, eventualValues) = updates.unzip3

    val stateCache = stateCacheBuilder(cache)
    Await.result(
      Future.sequence(Random.shuffle(updates).map { case (idx, key, eventualValue) =>
        Future {
          stateCache.feedAsync(key, idx, eventualValue.future)
        }
      }),
      10.seconds,
    )
    // All concurrent requests finish after 1 second
    Thread.sleep(1000L)
    Random.shuffle(indices zip eventualValues).foreach { case (idx, promisedString) =>
      promisedString.completeWith(Future {
        s"completed-$idx"
      })
    }

    caffeineCache.asMap().asScala should contain theSameElementsAs Map(
      "same-key" -> "completed-10000"
    )
  }
}
