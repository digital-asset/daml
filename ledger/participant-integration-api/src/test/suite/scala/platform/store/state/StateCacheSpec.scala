package com.daml.platform.store.state

import com.daml.caching.CaffeineCache.SimpleCaffeineCache
import com.daml.caching.{Cache => DamlCache}
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Success

class StateCacheSpec extends AnyFlatSpec with Matchers with MockitoSugar with Eventually {
  behavior of "async cache loading"

  private val stateCacheBuilder = (provided: DamlCache[String, String]) =>
    new StateCache[String, String] {
      override protected implicit def ec: ExecutionContext =
        scala.concurrent.ExecutionContext.global

      override def cache: DamlCache[String, String] = provided
    }

  it should "load async" in {
    val caffeineCache: Cache[String, String] = Caffeine
      .newBuilder()
      .maximumSize(3)
      .build()
    val cache = new SimpleCaffeineCache[String, String](caffeineCache)
    val updates = (1 to 10).map(idx => (idx.toLong, "same-key", Promise[String]))
    val (indices, _, eventualValues) = updates.unzip3

    val stateCache = stateCacheBuilder(cache)
    updates.foreach { case (idx, key, eventualValue) =>
      stateCache.feedAsync(key, idx, eventualValue.future)
    }
    (indices zip eventualValues).sortWith { case (a, b) => a._1 > b._1 }.foreach {
      case (idx, promisedString) =>
        promisedString.complete(Success(s"completed-$idx"))
    }

    caffeineCache.asMap().asScala should contain theSameElementsAs Map(
      "same-key" -> "completed-10"
    )
  }
}
