// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.math.BigInteger
import java.nio.charset.Charset
import java.security.MessageDigest

import com.daml.platform.store.FlywayMigrationsSpec._
import org.apache.commons.io.IOUtils
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.core.api.migration.JavaMigration
import org.flywaydb.core.internal.resource.LoadableResource
import org.flywaydb.core.internal.scanner.{ResourceNameCache, Scanner}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._

// SQL MIGRATION AND THEIR DIGEST FILES SHOULD BE CREATED ONLY ONCE AND NEVER CHANGED AGAIN,
// OTHERWISE MIGRATIONS BREAK ON EXISTING DEPLOYMENTS!
class FlywayMigrationsSpec extends WordSpec {

  "Postgres flyway migration files" should {
    "always have a valid SHA-256 digest file accompanied" in {
      assertFlywayMigrationFileHashes(DbType.Postgres)
    }
  }

  "H2 database flyway migration files" should {
    "always have a valid SHA-256 digest file accompanied" in {
      assertFlywayMigrationFileHashes(DbType.H2Database)
    }
  }

}

object FlywayMigrationsSpec {

  private val digester = MessageDigest.getInstance("SHA-256")

  private def assertFlywayMigrationFileHashes(dbType: DbType): Unit = {
    val config = FlywayMigrations.configurationBase(dbType)
    val resourceScanner = scanner(config)
    val resources = resourceScanner.getResources("", ".sql").asScala.toSeq
    resources.size should be > 10

    resources.foreach { res =>
      val fileName = res.getFilename
      val expectedDigest =
        getExpectedDigest(fileName, fileName.dropRight(4) + ".sha256", resourceScanner)
      val currentDigest = getCurrentDigest(res, config.getEncoding)

      assert(
        currentDigest == expectedDigest,
        s"Digest of migration file $fileName has changed! It is NOT allowed to change neither existing sql migrations files nor their digests!"
      )
    }
  }

  private def scanner(config: FluentConfiguration) =
    new Scanner(
      classOf[JavaMigration],
      config.getLocations.toList.asJava,
      getClass.getClassLoader,
      config.getEncoding,
      new ResourceNameCache,
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
        .read())

  private def getCurrentDigest(res: LoadableResource, encoding: Charset) = {
    val digest = digester.digest(IOUtils.toByteArray(res.read(), encoding))
    String.format(s"%0${digest.length * 2}x\n", new BigInteger(1, digest))
  }
}
