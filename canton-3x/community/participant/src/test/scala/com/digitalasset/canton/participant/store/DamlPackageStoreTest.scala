// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String256M}
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, TestHash}
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.admin.PackageServiceTest.readCantonExamples
import com.digitalasset.canton.participant.store.DamlPackageStore.readPackageId
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

trait DamlPackageStoreTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  protected implicit def traceContext: TraceContext
  implicit val ec: ExecutionContextIdlenessExecutorService = parallelExecutionContext

  def damlPackageStore(mk: () => DamlPackageStore): Unit = {
    val damlPackages = readCantonExamples()
    val damlPackage = damlPackages(0)
    val packageId = DamlPackageStore.readPackageId(damlPackage)
    val damlPackage2 = damlPackages(1)
    val packageId2 = DamlPackageStore.readPackageId(damlPackage2)

    val darName = String255.tryCreate("CantonExamples")
    val darPath = this.getClass.getClassLoader.getResource(darName.unwrap + ".dar").getPath
    val darFile = new File(darPath)
    val darData = Files.readAllBytes(darFile.toPath)
    val hash = TestHash.digest("hash")

    val testDescription = String256M.tryCreate("test")
    val testDescription2 = String256M.tryCreate("other test description")

    "save, retrieve, and remove a dar" in {
      val store = mk()
      for {
        _ <- store
          .append(
            List(damlPackage),
            testDescription,
            Some(Dar(DarDescriptor(hash, darName), darData)),
          )
          .failOnShutdown
        result <- store.getDar(hash)
        pkg <- store.getPackage(packageId)
        _ <- store.removeDar(hash).failOnShutdown
        removed <- store.getDar(hash)
        pkgStillExists <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(Dar(DarDescriptor(hash, darName), darData))
        pkg shouldBe Some(damlPackage)
        removed shouldBe None
        pkgStillExists shouldBe Some(damlPackage)
      }
    }

    "list persisted dar hashes and filenames" in {
      val store = mk()
      for {
        _ <- store
          .append(
            List(damlPackage),
            testDescription,
            Some(Dar(DarDescriptor(hash, darName), darData)),
          )
          .failOnShutdown
        result <- store.listDars()
      } yield result should contain only DarDescriptor(hash, darName)
    }

    "preserve appended dar data even if the original one has the data modified" in {
      val store = mk()
      val dar = Dar(DarDescriptor(hash, darName), "dar contents".getBytes)
      for {
        _ <- store.append(List(damlPackage), testDescription, Some(dar)).failOnShutdown
        _ = ByteBuffer.wrap(dar.bytes).put("stuff".getBytes)
        result <- store.getDar(hash)
      } yield result shouldBe Some(Dar(DarDescriptor(hash, darName), "dar contents".getBytes))
    }

    "be able to persist the same dar many times at the same time" in {
      val store = mk()
      val dar = Dar(DarDescriptor(hash, darName), "dar contents".getBytes)
      for {
        _ <- Future.sequence(
          (0 until 4).map(_ =>
            store.append(List(damlPackage), testDescription, Some(dar)).failOnShutdown
          )
        )
        result <- store.getDar(hash)

        pkg <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(Dar(DarDescriptor(hash, darName), "dar contents".getBytes))
        pkg shouldBe Some(damlPackage)
      }
    }

    "insert and remove dars concurrently" in {
      val store = mk()

      val hash2 = TestHash.digest("hash2")
      val darName2 = String255.tryCreate("CantonTests")
      val counter = new AtomicInteger(0)

      val pkgsDar1 = damlPackages.take(2)
      val pkgsDar2 = damlPackages.drop(2)
      val deletedPkgs = damlPackages.dropRight(1)

      def runTest = {
        logger.info(s"Running iteration ${counter.getAndIncrement()} of concurrency test")

        val insert1: () => Future[Unit] = () =>
          store
            .append(
              pkgsDar1,
              testDescription,
              Some(Dar(DarDescriptor(hash, darName), darData)),
            )
            .failOnShutdown

        val insert2: () => Future[Unit] = () =>
          store
            .append(
              pkgsDar2,
              testDescription,
              Some(Dar(DarDescriptor(hash2, darName2), darData)),
            )
            .failOnShutdown

        val deletes: List[() => Future[Unit]] =
          deletedPkgs.map { pkg => () => store.removePackage(readPackageId(pkg)).failOnShutdown }

        val deleteDar2: () => Future[Unit] = () => store.removeDar(hash2).failOnShutdown

        // Shuffle the operations to add extra randomness to this test, as we're trying to test *concurrent* operations
        val operations =
          scala.util.Random.shuffle(List(insert1, insert2, deleteDar2) ++ deletes)

        val parallelFutures = operations.map(f => f())

        val remainingPackage = damlPackages.takeRight(1).headOption.value

        for {
          _units <- Future.sequence(parallelFutures)
          dar <- store.getDar(hash)
          pkg <- store.getPackage(readPackageId(remainingPackage))

          // Sanity check that the resulting state is sensible
          _ = dar shouldBe Some(Dar(DarDescriptor(hash, darName), darData))
          _ = pkg shouldBe Some(remainingPackage)

          // Cleanup for the next iteration
          _ <- Seq(hash, hash2).parTraverse(d => store.removeDar(d).failOnShutdown)
          _ <- (Seq(damlPackage, damlPackage2) ++ damlPackages.takeRight(2)).parTraverse(p =>
            store.removePackage(readPackageId(p)).failOnShutdown
          )

        } yield { succeed }

      }

      // Run 10 times to increase the chance of observing a concurrency bug
      MonadUtil.repeatFlatmap(Future(succeed), _unit => runTest, 10)
    }

    "save and retrieve one Daml Package" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), testDescription, None).failOnShutdown
        result <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(damlPackage)
      }
    }

    "save and retrieve multiple Daml Packages" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage, damlPackage2), testDescription, None).failOnShutdown
        resPkg2 <- store.getPackage(packageId2)
        resPkg1 <- store.getPackage(packageId)
      } yield {
        resPkg1 shouldBe Some(damlPackage)
        resPkg2 shouldBe Some(damlPackage2)
      }
    }

    "list package id and state of stored packages" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), testDescription, None).failOnShutdown
        result <- store.listPackages()
      } yield result should contain only PackageDescription(packageId, testDescription)
    }

    "be able to persist the same package many times at the same time" in {
      val store = mk()
      for {
        _ <- Future.sequence(
          (0 until 4).map(_ =>
            store.append(List(damlPackage), testDescription, None).failOnShutdown
          )
        )
        result <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(damlPackage)
      }
    }

    "list package ids by state" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), testDescription, None).failOnShutdown
        result <- store.listPackages()
      } yield result.loneElement shouldBe PackageDescription(packageId, testDescription)
    }

    "list package id and state of stored packages where sourceDescription is empty" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), String256M.empty, None).failOnShutdown
        result <- store.listPackages()
      } yield result should contain only PackageDescription(
        packageId,
        String256M.tryCreate("default"),
      )
    }

    "update a package description when (and only when) it is provided" in {
      val store = mk()
      for {
        _ <- store.append(List(damlPackage), testDescription, None).failOnShutdown
        pkg1 <- store.getPackageDescription(packageId)

        // Appending the same package with a new description should update it
        _ <- store.append(List(damlPackage), testDescription2, None).failOnShutdown
        pkg2 <- store.getPackageDescription(packageId)

        // Appending the same package without providing a description should leave it unchanged
        _ <- store.append(List(damlPackage), String256M.empty, None).failOnShutdown
        pkg3 <- store.getPackageDescription(packageId)

        // There are no duplicates
        pkgList <- store.listPackages()
      } yield {
        pkg1 shouldBe Some(PackageDescription(packageId, testDescription))
        pkg2 shouldBe Some(PackageDescription(packageId, testDescription2))
        pkg3 shouldBe Some(PackageDescription(packageId, testDescription2))
        pkgList.loneElement shouldBe PackageDescription(packageId, testDescription2)
      }
    }

    "list several packages with a limit" in {
      val store = mk()
      val test2Description = String256M.tryCreate("test2")
      for {
        _ <- store.append(List(damlPackage), testDescription, None).failOnShutdown
        _ <- store.append(List(damlPackage2), test2Description, None).failOnShutdown
        result1 <- store.listPackages(Some(1))
        result2 <- store.listPackages(Some(2))
      } yield {
        result1.loneElement should (
          be(PackageDescription(packageId, testDescription)) or
            be(PackageDescription(packageId2, test2Description))
        )
        result2.toSet shouldBe Set(
          PackageDescription(packageId, testDescription),
          PackageDescription(packageId2, test2Description),
        )
      }
    }

    "Store dar packages and use them to check for packages that need a dar upon removal" in {

      val store = mk()
      val packageIds = damlPackages.map(archive => readPackageId(archive))

      val fivePkgs = damlPackages.take(5)
      val missing3 = List(0, 1, 2, 4).map(i => damlPackages(i))
      val pkg3 = damlPackages(3)
      val withOrphan = packageIds.take(6)

      for {

        _ <- store
          .append(
            fivePkgs,
            testDescription,
            dar = Some(
              Dar(
                DamlPackageStoreTest.descriptor,
                DamlPackageStoreTest.descriptor.name.str.getBytes,
              )
            ),
          )
          .failOnShutdown
        _ <-
          store
            .append(
              missing3,
              testDescription,
              dar = Some(
                Dar(
                  DamlPackageStoreTest.descriptor2,
                  DamlPackageStoreTest.descriptor2.name.str.getBytes,
                )
              ),
            )
            .failOnShutdown

        _canRemoveDar2 <- noneOrFail(
          store
            .anyPackagePreventsDarRemoval(
              fivePkgs.map(readPackageId),
              removeDar = DamlPackageStoreTest.descriptor2,
            )
        )(s"Lookup packages with no dar other than ${DamlPackageStoreTest.descriptor2}")

        packageWithoutDar <-
          valueOrFail(
            store
              .anyPackagePreventsDarRemoval(
                fivePkgs.map(readPackageId),
                removeDar = DamlPackageStoreTest.descriptor,
              )
          )(s"Lookup packages with no dar other than ${DamlPackageStoreTest.descriptor}")

        // Check the case where all known packages have a "backup" DAR
        _ <- store
          .append(
            List(pkg3),
            testDescription,
            dar = Some(
              Dar(
                DamlPackageStoreTest.descriptor2,
                DamlPackageStoreTest.descriptor2.name.str.getBytes,
              )
            ),
          )
          .failOnShutdown
        _orphansExcluded <- noneOrFail(
          store
            .anyPackagePreventsDarRemoval(
              withOrphan,
              removeDar = DamlPackageStoreTest.descriptor,
            )
        )(s"orphan packages without DARs are excluded from `anyPackagePreventsDarRemoval`")

      } yield {
        packageWithoutDar shouldBe packageIds(3)
      }

    }

    "check for packages left without DARs when packages and DARs are removed" in {

      val store = mk()
      val fivePkgs = damlPackages.take(5)
      val removedPkg = damlPackages(2)
      val missing2and3 = List(0, 1, 4).map(i => damlPackages(i))
      val pkg3 = damlPackages(3)

      logger.info(s"All packages are ${fivePkgs.map(readPackageId)}")

      def canRemoveDar(descriptor: DarDescriptor) = {
        noneOrFail(
          store
            .anyPackagePreventsDarRemoval(
              fivePkgs.map(readPackageId),
              removeDar = descriptor,
            )
        )(s"Lookup packages with no dar other than ${descriptor}")

      }

      def canRemoveDar1 = canRemoveDar(DamlPackageStoreTest.descriptor)
      def canRemoveDar2 = canRemoveDar(DamlPackageStoreTest.descriptor2)

      def checkBothDarsCanBeRemoved = {
        for {
          _unit <- canRemoveDar1
          _unit <- canRemoveDar2
        } yield ()
      }

      for {

        _ <- store
          .append(
            fivePkgs,
            testDescription,
            dar = Some(
              Dar(
                DamlPackageStoreTest.descriptor,
                DamlPackageStoreTest.descriptor.name.str.getBytes,
              )
            ),
          )
          .failOnShutdown
        _ <- store
          .append(
            fivePkgs,
            testDescription,
            dar = Some(
              Dar(
                DamlPackageStoreTest.descriptor2,
                DamlPackageStoreTest.descriptor2.name.str.getBytes,
              )
            ),
          )
          .failOnShutdown

        _ = logger.info(s"All packages can initially be found in both DARs")
        _ <- checkBothDarsCanBeRemoved

        _ <- store.removePackage(readPackageId(removedPkg)).failOnShutdown

        _ = logger.info(
          s"After removing $removedPkg, all registered packages still come from two DARs"
        )
        _ <- checkBothDarsCanBeRemoved

        _ <- store.removeDar(DamlPackageStoreTest.descriptor.hash).failOnShutdown

        _ = logger.info(
          s"After removing ${DamlPackageStoreTest.descriptor}, we can no longer remove ${DamlPackageStoreTest.descriptor2}"
        )
        cantRemoveDar2 <-
          valueOrFail(
            store
              .anyPackagePreventsDarRemoval(
                fivePkgs.map(readPackageId),
                removeDar = DamlPackageStoreTest.descriptor2,
              )
          )(s"Lookup packages with no dar other than ${DamlPackageStoreTest.descriptor}")

        _ = logger.info(s"DARs that aren't registered can be removed")
        _unit <- canRemoveDar1

        _ = logger.info(
          s"Re-add ${DamlPackageStoreTest.descriptor}, missing the package ${readPackageId(pkg3)}"
        )
        _ <- store
          .append(
            missing2and3,
            testDescription,
            dar = Some(
              Dar(
                DamlPackageStoreTest.descriptor,
                DamlPackageStoreTest.descriptor.name.str.getBytes,
              )
            ),
          )
          .failOnShutdown

        _ = logger.info(s"We can still remove ${DamlPackageStoreTest.descriptor}")
        _unit <- canRemoveDar1

        _ = logger.info(
          s"We still can't remove ${DamlPackageStoreTest.descriptor2} as ${pkg3} has no alternative DAR"
        )
        stillCantRemoveDar2 <-
          valueOrFail(
            store
              .anyPackagePreventsDarRemoval(
                fivePkgs.map(readPackageId),
                removeDar = DamlPackageStoreTest.descriptor2,
              )
          )(s"Lookup packages with no dar other than ${DamlPackageStoreTest.descriptor}")

        _ = logger.info(s"Remove the problematic package $pkg3")
        _ <- store.removePackage(readPackageId(pkg3)).failOnShutdown

        _ = logger.info(
          s"Now all registered packages are again present in two DARS, so either DAR could be removed"
        )
        _unit <- checkBothDarsCanBeRemoved

      } yield {
        assert(fivePkgs.map(readPackageId).contains(cantRemoveDar2))
        stillCantRemoveDar2 shouldBe readPackageId(pkg3)
      }
    }

  }

}
object DamlPackageStoreTest {

  def descriptorFor(name: String): DarDescriptor = {

    val hash: Hash = Hash.digest(
      HashPurpose.DarIdentifier,
      ByteString.copyFromUtf8(name),
      HashAlgorithm.Sha256,
    )
    DarDescriptor(hash, String255.tryCreate(name))
  }

  val descriptor: DarDescriptor = descriptorFor("this-is-a-dar-name")
  val descriptor2: DarDescriptor = descriptorFor("this-is-a-different-dar-name")
}
