// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.packages

import com.daml.lf2.archive.daml_lf_dev.DamlLf
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref.PackageId
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.ledger.resources.TestResourceContext
import com.digitalasset.canton.metrics.CantonLabeledMetricsFactory.NoOpMetricsFactory
import com.digitalasset.canton.testing.utils.{TestModels, TestResourceUtils}
import org.apache.pekko.actor.{ActorSystem, Scheduler}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}
import scala.util.chaining.*

class DeduplicatingPackageLoaderSpec
    extends AsyncWordSpec
    with Matchers
    with TestResourceContext
    with BeforeAndAfterEach {

  private[this] var actorSystem: ActorSystem = _
  private[this] val loadCount = new AtomicLong()
  private[this] val metric = NoOpMetricsFactory.timer(MetricName("test-metric"))

  private[this] val dar =
    TestModels.com_daml_ledger_test_ModelTestDar_path
      .pipe(TestResourceUtils.resourceFile)
      .pipe(DarParser.assertReadArchiveFromFile(_))

  private[this] def delayedLoad(duration: FiniteDuration): Future[Option[DamlLf.Archive]] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler
    loadCount.incrementAndGet()
    org.apache.pekko.pattern.after(duration, scheduler) {
      Future.successful(Some(dar.main))
    }
  }

  private[this] def delayedFail(duration: FiniteDuration): Future[Option[DamlLf.Archive]] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler
    loadCount.incrementAndGet()
    org.apache.pekko.pattern.after(duration, scheduler) {
      Future.failed(new RuntimeException("Simulated package load failure"))
    }
  }

  private[this] def delayedNotFound(duration: FiniteDuration): Future[Option[DamlLf.Archive]] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler
    loadCount.incrementAndGet()
    org.apache.pekko.pattern.after(duration, scheduler) {
      Future.successful(None)
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    actorSystem = ActorSystem(getClass.getSimpleName)
    loadCount.set(0)
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
        _ = Threading.sleep(100, 0)
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
        _ = Threading.sleep(100, 0)
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
