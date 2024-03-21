// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection

import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

private[backend] trait StorageBackendTestsIngestion
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (ingestion)"

  import StorageBackendTestValues._

  it should "ingest a single configuration update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoConfiguration(someOffset, someConfiguration)
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    val configBeforeLedgerEndUpdate = executeSql(backend.configuration.ledgerConfiguration)
    executeSql(
      updateLedgerEnd(someOffset, 0)
    )
    val configAfterLedgerEndUpdate = executeSql(backend.configuration.ledgerConfiguration)

    // The first query is executed before the ledger end is updated.
    // It should not see the already ingested configuration change.
    configBeforeLedgerEndUpdate shouldBe empty

    // The second query should now see the configuration change.
    inside(configAfterLedgerEndUpdate) { case Some((offset, config)) =>
      offset shouldBe someOffset
      config shouldBe someConfiguration
    }
    configAfterLedgerEndUpdate should not be empty
  }

  it should "ingest a single package update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoPackage(someOffset),
      dtoPackageEntry(someOffset),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    val packagesBeforeLedgerEndUpdate = executeSql(backend.packageBackend.lfPackages)
    executeSql(
      updateLedgerEnd(someOffset, 0)
    )
    val packagesAfterLedgerEndUpdate = executeSql(backend.packageBackend.lfPackages)

    // The first query is executed before the ledger end is updated.
    // It should not see the already ingested package upload.
    packagesBeforeLedgerEndUpdate shouldBe empty

    // The second query should now see the package.
    packagesAfterLedgerEndUpdate should not be empty
  }

  it should "ingest a single party update" in {
    val someOffset = offset(1)
    val dtos = Vector(
      dtoPartyEntry(someOffset)
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    val partiesBeforeLedgerEndUpdate = executeSql(backend.party.knownParties)
    executeSql(
      updateLedgerEnd(someOffset, 0)
    )
    val partiesAfterLedgerEndUpdate = executeSql(backend.party.knownParties)

    // The first query is executed before the ledger end is updated.
    // It should not see the already ingested party allocation.
    partiesBeforeLedgerEndUpdate shouldBe empty

    // The second query should now see the party.
    partiesAfterLedgerEndUpdate should not be empty
  }

  private val NumberOfUpsertPackagesTests = 30
  it should s"safely upsert packages concurrently ($NumberOfUpsertPackagesTests)" in withConnections(
    2
  ) { connections =>
    import scala.concurrent.ExecutionContext.Implicits.global

    val List(connection1, connection2) = connections
    def packageFor(n: Int): DbDto.Package =
      dtoPackage(offset(n.toLong))
        .copy(
          package_id = s"abc123$n",
          _package = Random.nextString(Random.nextInt(200000) + 200).getBytes,
        )
    val conflictingPackageDtos = 11 to 20 map packageFor
    val packages1 = 21 to 30 map packageFor
    val packages2 = 31 to 40 map packageFor

    def test() = {

      executeSql(backend.parameter.initializeParameters(someIdentityParams))

      def ingestPackagesF(connection: Connection, packages: Iterable[DbDto.Package]): Future[Unit] =
        Future {
          connection.setAutoCommit(false)
          ingest(packages.toVector, connection)
          connection.commit()
        }

      val ingestF1 = ingestPackagesF(connection1, packages1 ++ conflictingPackageDtos)
      val ingestF2 = ingestPackagesF(connection2, packages2 ++ conflictingPackageDtos)

      Await.result(ingestF1, Duration(10, "seconds"))
      Await.result(ingestF2, Duration(10, "seconds"))

      executeSql(updateLedgerEnd(offset(50), 0))

      executeSql(backend.packageBackend.lfPackages).keySet.map(_.toString) shouldBe (
        conflictingPackageDtos ++ packages1 ++ packages2
      ).map(_.package_id).toSet
    }

    1 to NumberOfUpsertPackagesTests foreach { _ =>
      test()
      executeSql(backend.reset.resetAll)
    }
  }
}
