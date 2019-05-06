// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.migration

import java.math.BigInteger
import java.security.MessageDigest

import com.digitalasset.platform.sandbox.stores.ledger.sql.migration.FlywayMigrations.configurationBase
import org.flywaydb.core.internal.resource.LoadableResource
import org.flywaydb.core.internal.scanner.Scanner
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

// SQL MIGRATION AND THEIR DIGEST FILES SHOULD BE CREATED ONLY ONCE AND NEVER CHANGED AGAIN,
// OTHERWISE MIGRATIONS BREAK ON EXISTING DEPLOYMENTS!
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.StringPlusAny"))
class FlywayMigrationsSpec extends WordSpec with Matchers {

  private val digester = MessageDigest.getInstance("SHA-256")

  private val resourceScanner = new Scanner(
    configurationBase.getLocations.toList.asJava,
    getClass.getClassLoader,
    configurationBase.getEncoding
  )

  "Flyway migration files" should {
    "always have a valid SHA-256 digest file accompanied" in {
      resourceScanner
        .getResources("", ".sql")
        .asScala
        .map { res =>
          val fileName = res.getFilename
          val expectedDigest = getExpectedDigest(fileName, fileName.dropRight(4) + ".sha256")
          val currentDigest = getCurrentDigest(res)

          assert(
            currentDigest == expectedDigest,
            s"Digest of migration file $fileName has changed! It is NOT allowed to change neither existing sql migrations files nor their digests!"
          )
        }
    }
  }

  private def getExpectedDigest(sourceFile: String, digestFile: String) =
    new String(Option(resourceScanner.getResource(digestFile))
      .getOrElse(sys.error(
        s"Missing sha-256 file $digestFile! Are you introducing a new Flyway migration step? You need to create a sha-256 digest file by running this under the db/migration folder: shasum -a 256 $sourceFile | awk '{print $$1}' > $digestFile"))
      .loadAsBytes())

  private def getCurrentDigest(res: LoadableResource) = {
    val digest = digester.digest(res.loadAsBytes())
    val bi = new BigInteger(1, digest)
    (String.format("%0" + (digest.length << 1) + "X", bi) + "\n").toLowerCase
  }
}
