// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.io.{BufferedReader, FileNotFoundException}
import java.math.BigInteger
import java.nio.charset.Charset
import java.nio.file.Paths
import java.security.MessageDigest
import java.util

import com.daml.ledger.on.sql.ImmutableMigrationsSpec._
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.core.internal.resource.LoadableResource
import org.flywaydb.core.internal.scanner.{ResourceNameCache, Scanner}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._

class ImmutableMigrationsSpec extends WordSpec {
  "migration files" should {
    "never change, according to their accompanying digest file" in {
      val configuration = Flyway
        .configure()
        .locations(s"classpath:/$migrationsResourcePath")
      val resourceScanner = flywayScanner(configuration)
      val resources = resourceScanner.getResources("", ".sql").asScala.toSeq
      resources.size should be >= 3

      resources.foreach { resource =>
        val migrationFile = resource.getRelativePath
        val digestFile = migrationFile + ".sha256"
        val expectedDigest = readExpectedDigest(migrationFile, digestFile, resourceScanner)
        val currentDigest = computeCurrentDigest(resource, configuration.getEncoding)
        assert(
          currentDigest == expectedDigest,
          s"""The contents of the migration file "$migrationFile" have changed! Migrations are immutable; you must not change their contents or their digest.""",
        )
      }
    }
  }
}

object ImmutableMigrationsSpec {
  private val migrationsResourcePath = "com/daml/ledger/on/sql/migrations"
  private val migrationsDirectoryPath =
    Paths.get(s"ledger/ledger-on-sql/src/main/resources/$migrationsResourcePath")

  private def flywayScanner(configuration: FluentConfiguration) =
    new Scanner(
      classOf[Object],
      util.Arrays.asList(configuration.getLocations: _*),
      getClass.getClassLoader,
      configuration.getEncoding,
      new ResourceNameCache,
    )

  private def readExpectedDigest(
      sourceFile: String,
      digestFile: String,
      resourceScanner: Scanner[_],
  ): String = {
    val resource = Option(resourceScanner.getResource(digestFile))
      .getOrElse(
        throw new FileNotFoundException(
          s""""$digestFile" is missing. If you are introducing a new Flyway migration step, you need to create an SHA-256 digest file by running:
           |shasum -a 256 '${migrationsDirectoryPath.resolve(sourceFile)}' \\
           |  | awk '{print $$1}' \\
           |  > '${migrationsDirectoryPath.resolve(digestFile)}'
           |""".stripMargin,
        ),
      )
    new BufferedReader(resource.read()).readLine()
  }

  private def computeCurrentDigest(resource: LoadableResource, encoding: Charset): String = {
    val sha256 = MessageDigest.getInstance("SHA-256")
    new BufferedReader(resource.read())
      .lines()
      .forEach(line => sha256.update((line + "\n").getBytes(encoding)))
    val digest = sha256.digest()
    String.format(s"%0${digest.length * 2}x", new BigInteger(1, digest))
  }
}
