// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.packages

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ActorSystem, Scheduler}
import com.codahale.metrics.MetricRegistry
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.archive.DarReader
import com.daml.lf.data.Ref.PackageId
import com.daml.platform.testing.LogCollector
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

class DeduplicatingPackageLoaderSpec
    extends AsyncWordSpec
    with Matchers
    with TestResourceContext
    with BeforeAndAfterEach {

  private[this] var actorSystem: ActorSystem = _
  private[this] val loadCount = new AtomicLong()
  private[this] val metricRegistry = new MetricRegistry
  private[this] val metric = metricRegistry.timer("test-metric")

  private[this] val reader = DarReader { (_, stream) =>
    Try(DamlLf.Archive.parseFrom(stream))
  }
  private[this] val Success(archive) =
    reader.readArchiveFromFile(new File(rlocation("ledger/test-common/model-tests.dar")))

  private[this] def delayedLoad(duration: FiniteDuration): Future[Option[DamlLf.Archive]] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler
    loadCount.incrementAndGet()
    akka.pattern.after(duration, scheduler) {
      Future.successful(Some(archive.main))
    }
  }

  private[this] def delayedFail(duration: FiniteDuration): Future[Option[DamlLf.Archive]] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler
    loadCount.incrementAndGet()
    akka.pattern.after(duration, scheduler) {
      Future.failed(new RuntimeException("Simulated package load failure"))
    }
  }

  private[this] def delayedNotFound(duration: FiniteDuration): Future[Option[DamlLf.Archive]] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler
    loadCount.incrementAndGet()
    akka.pattern.after(duration, scheduler) {
      Future.successful(None)
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    actorSystem = ActorSystem(getClass.getSimpleName)
    loadCount.set(0)
    LogCollector.clear[this.type]
  }

  override def afterEach(): Unit = {
    Await.result(actorSystem.terminate(), 10.seconds)
    super.afterEach()
  }

  "DeduplicatingPackageLoaderSpec" should {
    "correctly load two different packages" in {
      val loader = new DeduplicatingPackageLoader()
      val packageId1 = PackageId.assertFromString(UUID.randomUUID().toString)
      val packageId2 = PackageId.assertFromString(UUID.randomUUID().toString)
      val future1 = loader.loadPackage(packageId1, _ => delayedLoad(200.millis), metric)
      val future2 = loader.loadPackage(packageId2, _ => delayedLoad(200.millis), metric)
      for {
        result1 <- future1
        result2 <- future2
      } yield {
        result1.isDefined shouldBe true
        result2.isDefined shouldBe true

        // 2 successful
        loadCount.get() shouldBe 2
      }
    }

    "deduplicate concurrent package load requests" in {
      val loader = new DeduplicatingPackageLoader()
      val packageId = PackageId.assertFromString(UUID.randomUUID().toString)
      val future1 = loader.loadPackage(packageId, _ => delayedLoad(200.millis), metric)
      val future2 = loader.loadPackage(packageId, _ => delayedLoad(200.millis), metric)
      for {
        result1 <- future1
        result2 <- future2
      } yield {
        result1.isDefined shouldBe true
        result2.isDefined shouldBe true

        // 1 successful, 1 deduplicated
        loadCount.get() shouldBe 1
      }
    }

    "deduplicate sequential package load requests" in {
      val loader = new DeduplicatingPackageLoader()
      val packageId = PackageId.assertFromString(UUID.randomUUID().toString)
      for {
        result1 <- loader.loadPackage(packageId, _ => delayedLoad(10.millis), metric)
        result2 <- loader.loadPackage(packageId, _ => delayedLoad(10.millis), metric)
      } yield {
        result1.isDefined shouldBe true
        result2.isDefined shouldBe true

        // 1 successful, 1 deduplicated
        loadCount.get() shouldBe 1
      }
    }

    "retry after a failed package load requests" in {
      val loader = new DeduplicatingPackageLoader()
      val packageId = PackageId.assertFromString(UUID.randomUUID().toString)
      for {
        _ <- loader.loadPackage(packageId, _ => delayedFail(10.millis), metric).failed
        // Wait for a short time so that the package loader can remove the failed load from the cache.
        // Without the wait, the second call might get the failed result from above
        _ = Thread.sleep(100, 0)
        result2 <- loader.loadPackage(packageId, _ => delayedLoad(10.millis), metric)
      } yield {
        result2.isDefined shouldBe true

        // 1 failed, 1 successful
        loadCount.get() shouldBe 2
      }
    }

    "retry after a package was not found" in {
      val loader = new DeduplicatingPackageLoader()
      val packageId = PackageId.assertFromString(UUID.randomUUID().toString)
      for {
        result1 <- loader.loadPackage(packageId, _ => delayedNotFound(10.millis), metric)
        _ = Thread.sleep(100, 0)
        result2 <- loader.loadPackage(packageId, _ => delayedLoad(10.millis), metric)
      } yield {
        result1 shouldBe None
        result2.isDefined shouldBe true

        // 1 package not found, 1 successful
        loadCount.get() shouldBe 2
      }
    }
  }
}
