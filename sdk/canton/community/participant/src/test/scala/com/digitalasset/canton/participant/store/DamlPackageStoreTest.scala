// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.PackageDescription
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.admin.PackageService.{
  Dar,
  DarDescription,
  DarMainPackageId,
}
import com.digitalasset.canton.participant.admin.PackageServiceTest.readCantonExamples
import com.digitalasset.canton.participant.store.DamlPackageStore.readPackageId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext, InUS, LfPackageId}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

trait DamlPackageStoreTest extends AsyncWordSpec with BaseTest with HasExecutionContext with InUS {

  protected implicit def traceContext: TraceContext
  implicit val ec: ExecutionContextIdlenessExecutorService = parallelExecutionContext

  def damlPackageStore(mk: () => DamlPackageStore): Unit = {
    val damlPackages = readCantonExamples()
    val damlPackage = damlPackages.head
    val damlPackageSize1 = damlPackage.getPayload.size()
    val packageInfo =
      PackageInfo(String255.tryCreate("CantonExamples"), String255.tryCreate("1.0.0"))
    val packageId = DamlPackageStore.readPackageId(damlPackage)
    val damlPackage2 = damlPackages(1)
    val damlPackageSize2 = damlPackage2.getPayload.size()
    val packageInfo2 =
      PackageInfo(String255.tryCreate("CantonExamples"), String255.tryCreate("1.0.0"))
    val packageId2 = DamlPackageStore.readPackageId(damlPackage2)

    val uploadedAt = CantonTimestamp.now()
    val darDescription = String255.tryCreate("CantonExamples")
    val darPath = this.getClass.getClassLoader.getResource(darDescription.unwrap + ".dar").getPath
    val darFile = new File(darPath)
    val darData = Files.readAllBytes(darFile.toPath)
    val mainPackageId = DarMainPackageId.tryCreate(packageId)
    val dar = Dar(
      DarDescription(
        mainPackageId,
        description = darDescription,
        name = String255.tryCreate("CantonExamples"),
        version = String255.tryCreate("1.0.0"),
      ),
      darData,
    )

    "save, retrieve, and remove a dar" inUS {
      val store = mk()
      for {
        _ <- store
          .append(
            List((packageInfo, damlPackage)),
            uploadedAt,
            dar,
          )
        result <- store.getDar(mainPackageId).value
        pkg <- store.getPackage(packageId)
        _ <- store.removeDar(mainPackageId)
        removed <- store.getDar(mainPackageId).value
        pkgStillExists <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(dar)
        pkg shouldBe Some(damlPackage)
        removed shouldBe None
        pkgStillExists shouldBe Some(damlPackage)
      }
    }

    "list persisted dar main package-ids and filenames" inUS {
      val store = mk()
      for {
        _ <- store
          .append(
            List((packageInfo, damlPackage)),
            uploadedAt,
            dar,
          )
        result <- store.listDars()
      } yield result should contain only DarDescription(
        mainPackageId,
        description = darDescription,
        name = packageInfo.name,
        version = packageInfo.version,
      )
    }

    "provide stored packages in dar" inUS {
      val store = mk()
      for {
        _ <-
          store
            .append(
              List((packageInfo, damlPackage)),
              uploadedAt,
              dar,
            )
        packages <- store
          .getPackageDescriptionsOfDar(dar.descriptor.mainPackageId)
          .value
          .map(_.valueOrFail("unable to find dar"))
        dars <- store.getPackageReferences(
          LfPackageId.assertFromString(dar.descriptor.mainPackageId.str)
        )
      } yield {
        packages.map(_.packageId) should contain(dar.descriptor.mainPackageId.str)
        dars.map(_.mainPackageId) shouldBe Seq(dar.descriptor.mainPackageId)
      }
    }

    "preserve appended dar data even if the original one has the data modified" inUS {
      val store = mk()
      val dar = Dar(
        DarDescription(
          mainPackageId,
          description = darDescription,
          name = packageInfo.name,
          version = packageInfo.version,
        ),
        "dar contents".getBytes,
      )
      for {
        _ <- store.append(List((packageInfo, damlPackage)), uploadedAt, dar)
        _ = ByteBuffer.wrap(dar.bytes).put("stuff".getBytes)
        result <- store.getDar(mainPackageId).value
      } yield result shouldBe Some(
        Dar(
          DarDescription(
            mainPackageId,
            description = darDescription,
            name = packageInfo.name,
            version = packageInfo.version,
          ),
          "dar contents".getBytes,
        )
      )
    }

    "be able to persist the same dar many times at the same time" inUS {
      val store = mk()
      val dar = Dar(
        DarDescription(
          mainPackageId,
          description = darDescription,
          name = packageInfo.name,
          version = packageInfo.version,
        ),
        "dar contents".getBytes,
      )
      for {
        _ <- FutureUnlessShutdown.sequence(
          (0 until 4).map(_ => store.append(List((packageInfo, damlPackage)), uploadedAt, dar))
        )
        result <- store.getDar(mainPackageId).value

        pkg <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(
          Dar(
            DarDescription(
              mainPackageId,
              description = darDescription,
              name = packageInfo.name,
              version = packageInfo.version,
            ),
            "dar contents".getBytes,
          )
        )
        pkg shouldBe Some(damlPackage)
      }
    }

    "insert and remove dars concurrently" inUS {
      val store = mk()

      val darId2 = DarMainPackageId.tryCreate("darId2")
      val darName2 = String255.tryCreate("CantonTests")
      val counter = new AtomicInteger(0)

      val pkgsDar1 = damlPackages.take(2).map((packageInfo, _))
      val pkgsDar2 = damlPackages.drop(2).map((packageInfo, _))
      val deletedPkgs = damlPackages.dropRight(1)

      def runTest = {
        logger.info(s"Running iteration ${counter.getAndIncrement()} of concurrency test")

        val insert1: () => FutureUnlessShutdown[Unit] = () =>
          store
            .append(
              pkgsDar1,
              uploadedAt,
              dar,
            )

        val insert2: () => FutureUnlessShutdown[Unit] = () =>
          store
            .append(
              pkgsDar2,
              uploadedAt,
              Dar(
                DarDescription(
                  darId2,
                  description = darName2,
                  name = packageInfo2.name,
                  version = packageInfo2.version,
                ),
                darData,
              ),
            )

        val deletes: List[() => FutureUnlessShutdown[Unit]] =
          deletedPkgs.map(pkg => () => store.removePackage(readPackageId(pkg)))

        val deleteDar2: () => FutureUnlessShutdown[Unit] = () => store.removeDar(darId2)

        // Shuffle the operations to add extra randomness to this test, as we're trying to test *concurrent* operations
        val operations =
          scala.util.Random.shuffle(List(insert1, insert2, deleteDar2) ++ deletes)

        val parallelFutures = operations.map(f => f())

        val remainingPackage = damlPackages.takeRight(1).headOption.value

        for {
          _units <- FutureUnlessShutdown.sequence(parallelFutures)
          dar <- store.getDar(mainPackageId).value
          pkg <- store.getPackage(readPackageId(remainingPackage))

          // Sanity check that the resulting state is sensible
          _ = dar shouldBe dar
          _ = pkg shouldBe Some(remainingPackage)

          // Cleanup for the next iteration
          _ <- Seq(mainPackageId, darId2).parTraverse(d => store.removeDar(d))
          _ <- (Seq(damlPackage, damlPackage2) ++ damlPackages.takeRight(2)).parTraverse(p =>
            store.removePackage(readPackageId(p))
          )

        } yield { succeed }

      }

      // Run 10 times to increase the chance of observing a concurrency bug
      MonadUtil.repeatFlatmap(FutureUnlessShutdown.pure(succeed), _ => runTest, 10)
    }

    "save and retrieve one Daml Package" inUS {
      val store = mk()
      for {
        _ <- store.append(List((packageInfo, damlPackage)), uploadedAt, dar)
        result <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(damlPackage)
      }
    }

    "save and retrieve multiple Daml Packages" inUS {
      val store = mk()
      for {
        _ <- store.append(
          List((packageInfo, damlPackage), (packageInfo2, damlPackage2)),
          uploadedAt,
          dar,
        )
        resPkg2 <- store.getPackage(packageId2)
        resPkg1 <- store.getPackage(packageId)
      } yield {
        resPkg1 shouldBe Some(damlPackage)
        resPkg2 shouldBe Some(damlPackage2)
      }
    }

    "list package id and state of stored packages" inUS {
      val store = mk()
      for {
        _ <- store.append(List((packageInfo, damlPackage)), uploadedAt, dar)
        result <- store.listPackages()
      } yield result should contain only PackageDescription(
        packageId,
        name = packageInfo.name,
        version = packageInfo.version,
        uploadedAt,
        damlPackageSize1,
      )
    }

    "be able to persist the same package many times at the same time" inUS {
      val store = mk()
      for {
        _ <- FutureUnlessShutdown.sequence(
          (0 until 4).map(_ => store.append(List((packageInfo, damlPackage)), uploadedAt, dar))
        )
        result <- store.getPackage(packageId)
      } yield {
        result shouldBe Some(damlPackage)
      }
    }

    "list package ids by state" inUS {
      val store = mk()
      for {
        _ <- store.append(List((packageInfo, damlPackage)), uploadedAt, dar)
        result <- store.listPackages()
      } yield result.loneElement shouldBe PackageDescription(
        packageId,
        name = packageInfo.name,
        version = packageInfo.version,
        uploadedAt,
        damlPackageSize1,
      )
    }

    "list several packages with a limit" inUS {
      val store = mk()
      for {
        _ <- store.append(List((packageInfo, damlPackage)), uploadedAt, dar)
        _ <- store.append(List((packageInfo2, damlPackage2)), uploadedAt, dar)
        result1 <- store.listPackages(Some(1))
        result2 <- store.listPackages(Some(2))
      } yield {
        val r1 = PackageDescription(
          packageId,
          name = packageInfo.name,
          version = packageInfo.version,
          uploadedAt,
          damlPackageSize1,
        )
        val r2 = PackageDescription(
          packageId2,
          name = packageInfo2.name,
          version = packageInfo2.version,
          uploadedAt,
          damlPackageSize2,
        )
        result1.loneElement should (be(r1) or be(r2))
        result2.toSet shouldBe Set(r1, r2)
      }
    }

    "Store dar packages and use them to check for packages that need a dar upon removal" inUS {

      val store = mk()
      val packageIds = damlPackages.map(archive => readPackageId(archive))

      val fivePkgs = damlPackages.take(5)
      val missing3 = List(0, 1, 2, 4).map(i => damlPackages(i))
      val pkg3 = damlPackages(3)
      val withOrphan = packageIds.take(6)

      for {

        _ <- store
          .append(
            fivePkgs.map((packageInfo, _)),
            uploadedAt,
            dar = Dar(
              DamlPackageStoreTest.descriptor,
              DamlPackageStoreTest.descriptor.name.str.getBytes,
            ),
          )
        _ <-
          store
            .append(
              missing3.map((packageInfo, _)),
              uploadedAt,
              dar = Dar(
                DamlPackageStoreTest.descriptor2,
                DamlPackageStoreTest.descriptor2.name.str.getBytes,
              ),
            )

        _canRemoveDar2 <- noneOrFailUS(
          store
            .anyPackagePreventsDarRemoval(
              fivePkgs.map(readPackageId),
              removeDar = DamlPackageStoreTest.descriptor2,
            )
        )(s"Lookup packages with no dar other than ${DamlPackageStoreTest.descriptor2}")

        packageWithoutDar <-
          valueOrFailUS(
            store
              .anyPackagePreventsDarRemoval(
                fivePkgs.map(readPackageId),
                removeDar = DamlPackageStoreTest.descriptor,
              )
          )(s"Lookup packages with no dar other than ${DamlPackageStoreTest.descriptor}")

        // Check the case where all known packages have a "backup" DAR
        _ <- store
          .append(
            List(pkg3).map((packageInfo, _)),
            uploadedAt,
            dar = Dar(
              DamlPackageStoreTest.descriptor2,
              DamlPackageStoreTest.descriptor2.name.str.getBytes,
            ),
          )
        _orphansExcluded <- noneOrFailUS(
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

    "check for packages left without DARs when packages and DARs are removed" inUS {

      val store = mk()
      val fivePkgs = damlPackages.take(5)
      val removedPkg = damlPackages(2)
      val missing2and3 = List(0, 1, 4).map(i => damlPackages(i))
      val pkg3 = damlPackages(3)

      logger.info(s"All packages are ${fivePkgs.map(readPackageId)}")

      def canRemoveDar(descriptor: DarDescription) =
        noneOrFailUS(
          store
            .anyPackagePreventsDarRemoval(
              fivePkgs.map(readPackageId),
              removeDar = descriptor,
            )
        )(s"Lookup packages with no dar other than $descriptor")

      def canRemoveDar1: FutureUnlessShutdown[Assertion] =
        canRemoveDar(DamlPackageStoreTest.descriptor)
      def canRemoveDar2: FutureUnlessShutdown[Assertion] =
        canRemoveDar(DamlPackageStoreTest.descriptor2)

      def checkBothDarsCanBeRemoved: FutureUnlessShutdown[Unit] =
        for {
          _unit <- canRemoveDar1
          _unit <- canRemoveDar2
        } yield ()

      for {

        _ <- store
          .append(
            fivePkgs.map((packageInfo, _)),
            uploadedAt,
            dar = Dar(
              DamlPackageStoreTest.descriptor,
              DamlPackageStoreTest.descriptor.name.str.getBytes,
            ),
          )
        _ <- store
          .append(
            fivePkgs.map((packageInfo, _)),
            uploadedAt,
            dar = Dar(
              DamlPackageStoreTest.descriptor2,
              DamlPackageStoreTest.descriptor2.name.str.getBytes,
            ),
          )

        _ = logger.info(s"All packages can initially be found in both DARs")
        _ <- checkBothDarsCanBeRemoved

        _ <- store.removePackage(readPackageId(removedPkg))

        _ = logger.info(
          s"After removing $removedPkg, all registered packages still come from two DARs"
        )
        _ <- checkBothDarsCanBeRemoved

        _ <- store.removeDar(DamlPackageStoreTest.descriptor.mainPackageId)

        _ = logger.info(
          s"After removing ${DamlPackageStoreTest.descriptor}, we can no longer remove ${DamlPackageStoreTest.descriptor2}"
        )
        cantRemoveDar2 <-
          valueOrFailUS(
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
            missing2and3.map((packageInfo, _)),
            uploadedAt,
            dar = Dar(
              DamlPackageStoreTest.descriptor,
              DamlPackageStoreTest.descriptor.name.str.getBytes,
            ),
          )

        _ = logger.info(s"We can still remove ${DamlPackageStoreTest.descriptor}")
        _unit <- canRemoveDar1

        _ = logger.info(
          s"We still can't remove ${DamlPackageStoreTest.descriptor2} as $pkg3 has no alternative DAR"
        )
        stillCantRemoveDar2 <-
          valueOrFailUS(
            store
              .anyPackagePreventsDarRemoval(
                fivePkgs.map(readPackageId),
                removeDar = DamlPackageStoreTest.descriptor2,
              )
          )(s"Lookup packages with no dar other than ${DamlPackageStoreTest.descriptor}")

        _ = logger.info(s"Remove the problematic package $pkg3")
        _ <- store.removePackage(readPackageId(pkg3))

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

  def descriptorFor(name: String): DarDescription = {
    val str = String255.tryCreate(name)
    DarDescription(DarMainPackageId.tryCreate(name), str, str, str)
  }

  val descriptor: DarDescription = descriptorFor("this-is-a-dar-name")
  val descriptor2: DarDescription = descriptorFor("this-is-a-different-dar-name")
}
