// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.persistence

import java.io.StringWriter
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Instant

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.ledger.api.testing.utils.Resource
import org.apache.commons.io.{FileUtils, IOUtils}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.control.NonFatal

trait PostgresAroundAll extends PostgresAround with BeforeAndAfterAll {
  self: org.scalatest.Suite =>

  override protected def beforeAll(): Unit = {
    // we start pg before running the rest because _generally_ the database
    // needs to be up before everything else. this is relevant for
    // ScenarioLoadingITPostgres at least. we could much with the mixin
    // order but this was easier...
    postgresFixture = startEphemeralPg()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopAndCleanUp(postgresFixture.tempDir, postgresFixture.dataDir, postgresFixture.logFile)
  }
}

trait PostgresAroundEach extends PostgresAround with BeforeAndAfterEach {
  self: org.scalatest.Suite =>

  override protected def beforeEach(): Unit = {
    // we start pg before running the rest because _generally_ the database
    // needs to be up before everything else. this is relevant for
    // ScenarioLoadingITPostgres at least. we could much with the mixin
    // order but this was easier...
    postgresFixture = startEphemeralPg()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    stopAndCleanUp(postgresFixture.tempDir, postgresFixture.dataDir, postgresFixture.logFile)
  }

}

case class PostgresFixture(jdbcUrl: String, tempDir: Path, dataDir: Path, logFile: Path)

private class PostgresResource extends Resource[PostgresFixture] with PostgresAround {

  override def value: PostgresFixture = postgresFixture

  override def setup(): Unit = {
    postgresFixture = startEphemeralPg()
  }

  override def close(): Unit = {
    stopAndCleanUp(postgresFixture.tempDir, postgresFixture.dataDir, postgresFixture.logFile)
  }
}

object PostgresResource {
  def apply(): Resource[PostgresFixture] = new PostgresResource
}

trait PostgresAround {

  private val logger = LoggerFactory.getLogger(getClass)

  private val IS_OS_WINDOWS: Boolean = sys.props("os.name").toLowerCase contains "windows"

  protected val testUser = "test"

  @volatile
  protected var postgresFixture: PostgresFixture = null

  protected def startEphemeralPg(): PostgresFixture = {
    logger.info("starting Postgres fixture")
    val tempDir = Files.createTempDirectory("postgres_test")
    val tempDirPath = tempDir.toAbsolutePath.toString
    val dataDir = Paths.get(tempDirPath, "data")
    val postgresPort = findFreePort()

    def runInitDb() = {
      val command = Array(
        pgToolPath("initdb"),
        s"--username=$testUser",
        if (IS_OS_WINDOWS) "--locale=English_United States" else "--locale=en_US.UTF-8",
        "-E",
        "UNICODE",
        "-A",
        "trust",
        dataDir.toAbsolutePath.toString.replaceAllLiterally("\\", "/")
      )
      val initDbProcess = Runtime.getRuntime.exec(command)
      waitForItOrDie(initDbProcess, command.mkString(" "))
    }

    def createConfigFile() = {
      val postgresConf = Files.createFile(Paths.get(tempDirPath, "postgresql.conf"))

      // taken from here: https://bitbucket.org/eradman/ephemeralpg/src/1b5a3c6be81c69a860b7bd540a16b1249d3e50e2/pg_tmp.sh?at=default&fileviewer=file-view-default#pg_tmp.sh-54
      val configText =
        s"""|unix_socket_directories = '${tempDirPath}'
            |listen_addresses = ''
            |shared_buffers = 12
            |fsync = off
            |synchronous_commit = off
            |full_page_writes = off
            |log_min_duration_statement = 0
            |log_connections = on
      """.stripMargin

      Files.write(postgresConf, configText.getBytes(StandardCharsets.UTF_8))
    }

    def startPostgres() = {
      val logFile = Files.createFile(Paths.get(tempDirPath, "postgresql.log"))
      val command = Array(
        pgToolPath("pg_ctl"),
        "-o",
        s"-F -p $postgresPort",
        "-w",
        "-D",
        dataDir.toAbsolutePath.toString,
        "-l",
        logFile.toAbsolutePath.toString,
        //,
        "start"
      )
      val pgCtlStartProcess = Runtime.getRuntime.exec(command)
      waitForItOrDie(pgCtlStartProcess, command.mkString(" "))
      logFile
    }

    def createTestDatabase() = {
      val command = Array(
        pgToolPath("createdb"),
        "-U",
        testUser,
        "-p",
        postgresPort.toString,
        "test"
      )
      val createDbProcess = Runtime.getRuntime.exec(command)
      waitForItOrDie(createDbProcess, command.mkString(" "))
    }

    try {
      runInitDb()
      createConfigFile()
      val logFile = startPostgres()
      createTestDatabase()

      val jdbcUrl = s"jdbc:postgresql://localhost:$postgresPort/test?user=$testUser"

      PostgresFixture(jdbcUrl, tempDir, dataDir, logFile)
    } catch {
      case NonFatal(e) =>
        deleteTempFolder(tempDir)
        throw e
    }
  }

  private def waitForItOrDie(p: Process, what: String) = {
    logger.info(s"waiting for '$what' to exit")
    if (p.waitFor() != 0) {
      val writer = new StringWriter
      IOUtils.copy(p.getErrorStream, writer, "UTF-8")
      sys.error(writer.toString)
    }
    logger.info(s"the process has been terminated")
  }

  private def deleteTempFolder(tempDir: Path) =
    FileUtils.deleteDirectory(tempDir.toFile)

  private def findFreePort(): Int = {
    val s = new ServerSocket(0)
    val port = s.getLocalPort
    // we have to release the port so postgres can use it
    // note that there is a small window for race, as the release of the port and giving it to postgres is not atomic
    // if this turns out to be an issue, we need to find an atomic way of doing that
    s.close()
    port

  }

  protected def stopAndCleanUp(tempDir: Path, dataDir: Path, logFile: Path): Unit = {
    logger.info("stopping and cleaning up Postgres")
    val command = Array(
      pgToolPath("pg_ctl"),
      "-w",
      "-D",
      dataDir.toAbsolutePath.toString,
      "-m",
      "immediate",
      "stop"
    )
    val pgCtlStopProcess = Runtime.getRuntime.exec(command)

    try {
      waitForItOrDie(pgCtlStopProcess, command.mkString(" "))
    } catch {
      case ie: InterruptedException =>
        println(s"waitForItOrDie was interrupted at ${Instant.now().toString}!")
        println("postgres log:")
        Source
          .fromFile(logFile.toFile)
          .getLines()
          .foreach(s => println(s)) //otherwise getting wart (Any)
        throw ie
    }
    deleteTempFolder(tempDir)
  }

  private def pgToolPath(c: String): String = rlocation(
    s"external/postgresql_dev_env/bin/$c" + (if (IS_OS_WINDOWS) ".exe" else "")
  )

}
