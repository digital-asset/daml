// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.canton.platform.store.FlywayMigrationsSpec.*
import org.apache.commons.io.IOUtils
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.core.api.migration.JavaMigration
import org.flywaydb.core.api.resource.LoadableResource
import org.flywaydb.core.internal.scanner.{LocationScannerCache, ResourceNameCache, Scanner}
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.wordspec.AnyWordSpec

import java.math.BigInteger
import java.nio.charset.Charset
import scala.jdk.CollectionConverters.*

// SQL MIGRATION AND THEIR DIGEST FILES SHOULD BE CREATED ONLY ONCE AND NEVER CHANGED AGAIN,
// OTHERWISE MIGRATIONS BREAK ON EXISTING DEPLOYMENTS!
class FlywayMigrationsSpec extends AnyWordSpec {

  "Postgres flyway migration files" should {
    "always have a valid SHA-256 digest file accompanied" in {
      assertFlywayMigrationFileHashes(DbType.Postgres, 1)
    }
  }

  "H2 database flyway migration files" should {
    "always have a valid SHA-256 digest file accompanied" in {
      assertFlywayMigrationFileHashes(DbType.H2Database, 1)
    }
  }
}

object FlywayMigrationsSpec {

  private val digester = MessageDigestPrototype.Sha256.newDigest

  private def assertFlywayMigrationFileHashes(
      dbType: DbType,
      minMigrationCount: Int,
  ): Unit = {
    val config = Flyway
      .configure()
      .locations(FlywayMigrations.locations(dbType)*)
    val resourceScanner = scanner(config)
    val resources = resourceScanner.getResources("", ".sql").asScala.toSeq
    resources.size should be >= minMigrationCount

    // TODO(#16458) Remove these exceptions
    def skipCheck(filename: String): Boolean = {
      val skip = Seq("V1_1__initial", "V1_2__initial_views")
      skip.exists(filename.contains)
    }

    resources.collect {
      case res if !skipCheck(res.getFilename) =>
        val fileName = res.getFilename
        val expectedDigest =
          getExpectedDigest(fileName, fileName.dropRight(4) + ".sha256", resourceScanner)
        val currentDigest = getCurrentDigest(res, config.getEncoding)

        assert(
          currentDigest == expectedDigest,
          s"Digest of migration file $fileName has changed! It is NOT allowed to change neither existing sql migrations files nor their digests!",
        )
    }
  }

  private def scanner(config: FluentConfiguration) =
    new Scanner(
      classOf[JavaMigration],
      false,
      new ResourceNameCache,
      new LocationScannerCache,
      config,
    )

  private def getExpectedDigest(
      sourceFile: String,
      digestFile: String,
      resourceScanner: Scanner[_],
  ) =
    IOUtils.toString(
      Option(resourceScanner.getResource(digestFile))
        .getOrElse(sys.error(s"""Missing sha-256 file $digestFile!
           |Are you introducing a new Flyway migration step?
           |You need to create a sha-256 digest file by either running:
           | - shasum -a 256 $sourceFile | awk '{print $$1}' > $digestFile (under the db/migration folder)
           | - or ledger/sandbox/src/main/resources/db/migration/recompute-sha256sums.sh
           |""".stripMargin))
        .read()
    )

  private def getCurrentDigest(res: LoadableResource, encoding: Charset) = {
    val digest = digester.digest(IOUtils.toByteArray(res.read(), encoding))
    String.format(s"%0${digest.length * 2}x\n", new BigInteger(1, digest))
  }
}
