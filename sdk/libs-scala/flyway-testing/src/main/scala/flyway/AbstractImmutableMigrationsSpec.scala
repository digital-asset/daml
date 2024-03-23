// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.flyway

import com.daml.crypto.MessageDigestPrototype
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.core.api.resource.LoadableResource
import org.flywaydb.core.internal.scanner.{LocationScannerCache, ResourceNameCache, Scanner}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import java.io.{BufferedReader, FileNotFoundException}
import java.math.BigInteger
import java.nio.charset.Charset
import java.util
import scala.jdk.CollectionConverters._

abstract class AbstractImmutableMigrationsSpec extends AnyWordSpec {
  protected def migrationsResourcePath: String
  protected def migrationsMinSize: Int
  protected def hashMigrationsScriptPath: String

  private def flywayScanner(configuration: FluentConfiguration) =
    new Scanner(
      classOf[Object],
      util.Arrays.asList(configuration.getLocations: _*),
      getClass.getClassLoader,
      configuration.getEncoding,
      false,
      false,
      new ResourceNameCache,
      new LocationScannerCache,
      false,
    )

  private def readExpectedDigest(
      digestFile: String,
      resourceScanner: Scanner[_],
  ): String = {
    val resource = Option(resourceScanner.getResource(digestFile))
      .getOrElse(
        throw new FileNotFoundException(
          s"""\"$digestFile\" is missing. If you are introducing a new Flyway migration step, you need to create an SHA-256 digest file by running $hashMigrationsScriptPath."""
        )
      )
    new BufferedReader(resource.read()).readLine()
  }

  private def computeCurrentDigest(resource: LoadableResource, encoding: Charset): String = {
    val sha256 = MessageDigestPrototype.Sha256.newDigest
    new BufferedReader(resource.read())
      .lines()
      .forEach(line => sha256.update((line + "\n").getBytes(encoding)))
    val digest = sha256.digest()
    String.format(s"%0${digest.length * 2}x", new BigInteger(1, digest))
  }

  "migration files" should {
    "never change, according to their accompanying digest file" in {
      val configuration = Flyway
        .configure()
        .locations(s"classpath:/$migrationsResourcePath")
      val resourceScanner = flywayScanner(configuration)
      val resources = resourceScanner.getResources("", ".sql").asScala.toSeq
      resources.size should be >= migrationsMinSize

      resources.foreach { resource =>
        val migrationFile = resource.getRelativePath
        val digestFile = migrationFile + ".sha256"
        val expectedDigest = readExpectedDigest(digestFile, resourceScanner)
        val currentDigest = computeCurrentDigest(resource, configuration.getEncoding)
        assert(
          currentDigest == expectedDigest,
          s"""The contents of the migration file "$migrationFile" have changed! Migrations are immutable; you must not change their contents or their digest.""",
        )
      }
    }
  }
}
