package com.daml.platform.store.state

import com.daml.caching.CaffeineCache.SimpleCaffeineCache
import com.daml.caching.{Cache => DamlCache}
import com.daml.logging.LoggingContext
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.GenSeq
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

class StateCacheSpec extends AnyFlatSpec with Matchers with MockitoSugar with Eventually {
  behavior of "async cache loading"

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val `number of competing updates` = 1000
  private val `number of keys in cache` = 1000L

  it should "always store the latest key update in face of conflicting concurrent pending updates" in {
    val (stateCache, assertionSet) = setup()
    assertionSet.foreach(_._2.foreach(_.apply()))
    Thread.sleep(1000L)
    assertResults(stateCache, assertionSet)
  }

  it should "always store the latest key update in face of conflicting pending updates with significant duration" in {
    val (stateCache, assertionSet) = setup()
    Thread.sleep(1000L)
    assertionSet.foreach(_._2.foreach(_.apply()))
    Thread.sleep(1000L)
    assertResults(stateCache, assertionSet)
  }

  private val caffeineCache: Cache[String, String] = Caffeine
    .newBuilder()
    .maximumSize(`number of keys in cache`)
    .build()
  private val cache = new SimpleCaffeineCache[String, String](caffeineCache)

  private val stateCacheBuilder = (provided: DamlCache[String, String]) =>
    new StateCache[String, String] {
      override protected implicit def ec: ExecutionContext =
        scala.concurrent.ExecutionContext.global

      override def cache: DamlCache[String, String] = provided
    }

  private def setup() = {
    val stateCache = stateCacheBuilder(cache)
    val assertionSet = (1L to `number of keys in cache`).map { keyIdx =>
      val keyValue = s"some-key-$keyIdx"
      keyValue -> concurrentForKey(keyValue, `number of competing updates`, stateCache)
    }.par
    (stateCache, assertionSet)
  }

  private def assertResults(
      stateCache: StateCache[String, String],
      assertionSet: GenSeq[(String, GenSeq[() => Promise[String]])],
  ) = {
    caffeineCache.asMap().asScala should contain theSameElementsAs assertionSet.map {
      case (key, _) => key -> s"completed-${`number of competing updates`}"
    }
    stateCache.pendingUpdates.size shouldBe 0
  }

  private def concurrentForKey(
      key: String,
      concurrency: Int,
      stateCache: StateCache[String, String],
  ) = {
    val updates =
      (1 to concurrency).map(idx => (idx.toLong, key, Promise[String]))
    val (indices, _, eventualValues) = updates.unzip3

    Random.shuffle(updates).foreach { case (idx, key, eventualValue) =>
      stateCache.putAsync(key, idx, eventualValue.future)
    }
    Random
      .shuffle(indices zip eventualValues)
      .map { case (idx, promisedString) =>
        () => promisedString.completeWith(Future.successful(s"completed-$idx"))
      }
      .par
  }
}
