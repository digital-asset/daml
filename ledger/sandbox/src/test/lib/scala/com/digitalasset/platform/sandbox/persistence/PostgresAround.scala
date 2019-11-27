// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.persistence

import java.io.StringWriter
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.ledger.api.testing.utils.Resource
import org.apache.commons.io.{FileUtils, IOUtils}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

trait PostgresAroundAll extends PostgresAround with BeforeAndAfterAll {
  self: org.scalatest.Suite =>

  override protected def beforeAll(): Unit = {
    // we start pg before running the rest because _generally_ the database
    // needs to be up before everything else. this is relevant for
    // ScenarioLoadingITPostgres at least. we could much with the mixin
    // order but this was easier...
    startEphemeralPostgres()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stopAndCleanUpPostgres()
  }
}

trait PostgresAroundEach extends PostgresAround with BeforeAndAfterEach {
  self: org.scalatest.Suite =>

  override protected def beforeEach(): Unit = {
    // we start pg before running the rest because _generally_ the database
    // needs to be up before everything else. this is relevant for
    // ScenarioLoadingITPostgres at least. we could much with the mixin
    // order but this was easier...
    startEphemeralPostgres()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    stopAndCleanUpPostgres()
  }
}

case class PostgresFixture(
    jdbcUrl: String,
    port: Int,
    tempDir: Path,
    dataDir: Path,
    confFile: Path,
    logFile: Path,
)

private class PostgresResource extends Resource[PostgresFixture] with PostgresAround {

  override def value: PostgresFixture = postgresFixture

  override def setup(): Unit = {
    startEphemeralPostgres()
  }

  override def close(): Unit = {
    stopAndCleanUpPostgres()
  }
}

object PostgresResource {
  def apply(): Resource[PostgresFixture] = new PostgresResource
}

trait PostgresAround {

  import PostgresAround._

  @volatile
  protected var postgresFixture: PostgresFixture = _

  protected def startEphemeralPostgres(): Unit = {
    logger.info("Starting an ephemeral PostgreSQL instance...")
    val tempDir = Files.createTempDirectory("postgres_test")
    val dataDir = tempDir.resolve("data")
    val confFile = Paths.get(dataDir.toString, "postgresql.conf")
    val port = findFreePort()
    val jdbcUrl = s"jdbc:postgresql://localhost:$port/test?user=$testUser"
    val logFile = Files.createFile(tempDir.resolve("postgresql.log"))
    postgresFixture = PostgresFixture(jdbcUrl, port, tempDir, dataDir, confFile, logFile)

    try {
      initializeDatabase()
      createConfigFile()
      startPostgres()
      createTestDatabase()
      logger.info("PostgreSQL has started.")
    } catch {
      case NonFatal(e) =>
        deleteRecursively(tempDir)
        postgresFixture = null
        throw e
    }
  }

  protected def stopAndCleanUpPostgres(): Unit = {
    logger.info("Stopping and cleaning up PostgreSQL...")
    stopPostgres()
    deleteRecursively(postgresFixture.tempDir)
    logger.info("PostgreSQL has stopped, and the data directory has been deleted.")
  }

  protected def startPostgres(): Unit = run(
    "start PostgreSQL",
    Tool.pg_ctl,
    "-o",
    s"-F -p ${postgresFixture.port}",
    "-w",
    "-D",
    postgresFixture.dataDir.toString,
    "-l",
    postgresFixture.logFile.toString,
    "start",
  )

  protected def stopPostgres(): Unit = {
    run(
      "stop PostgreSQL",
      Tool.pg_ctl,
      "-w",
      "-D",
      postgresFixture.dataDir.toString,
      "-m",
      "immediate",
      "stop",
    )
  }

  private def initializeDatabase(): Unit = run(
    "initialize the PostgreSQL database",
    Tool.initdb,
    s"--username=$testUser",
    if (isWindows) "--locale=English_United States" else "--locale=en_US.UTF-8",
    "-E",
    "UNICODE",
    "-A",
    "trust",
    postgresFixture.dataDir.toString.replaceAllLiterally("\\", "/")
  )

  private def createConfigFile(): Unit = {
    // taken from here: https://bitbucket.org/eradman/ephemeralpg/src/1b5a3c6be81c69a860b7bd540a16b1249d3e50e2/pg_tmp.sh?at=default&fileviewer=file-view-default#pg_tmp.sh-54
    // We set unix_socket_directories to /tmp rather than tempDir
    // since the latter will refer to a temporary directory set by
    // Bazel which is too long (there is a limit on the length of unix domain
    // sockets). On Windows, unix domain sockets do not exist and
    // this option is ignored.
    val configText =
      s"""|unix_socket_directories = '/tmp'
        |shared_buffers = 12MB
        |fsync = off
        |synchronous_commit = off
        |full_page_writes = off
        |log_min_duration_statement = 0
        |log_connections = on
        |listen_addresses = 'localhost'
        |port = ${postgresFixture.port}
        """.stripMargin
    Files.write(postgresFixture.confFile, configText.getBytes(StandardCharsets.UTF_8))
    ()
  }

  private def createTestDatabase(): Unit = run(
    "create the database",
    Tool.createdb,
    "-h",
    "localhost",
    "-U",
    testUser,
    "-p",
    postgresFixture.port.toString,
    "test",
  )

  private def run(description: String, tool: Tool, args: String*): Unit = {
    val command = tool.path.toString +: args
    logger.debug(s"Running: ${command.mkString(" ")}")
    try {
      val process = Runtime.getRuntime.exec(command.toArray)
      if (process.waitFor() != 0) {
        val stdout = new StringWriter
        IOUtils.copy(process.getInputStream, stdout, StandardCharsets.UTF_8)
        val stderr = new StringWriter
        IOUtils.copy(process.getErrorStream, stderr, StandardCharsets.UTF_8)
        val logs = Files.readAllLines(postgresFixture.logFile).asScala
        throw new ProcessFailedException(
          description,
          command,
          Some(stdout.toString),
          Some(stderr.toString),
          logs,
        )
      }
    } catch {
      case e: ProcessFailedException =>
        throw e
      case NonFatal(e) =>
        val logs = Files.readAllLines(postgresFixture.logFile).asScala
        throw new ProcessFailedException(description, command, None, None, logs, e)
    }
  }

  private def deleteRecursively(tempDir: Path): Unit =
    FileUtils.deleteDirectory(tempDir.toFile)
}

object PostgresAround {
  private val logger = LoggerFactory.getLogger(getClass)

  private val testUser = "test"

  private val isWindows = sys.props("os.name").toLowerCase contains "windows"

  private def findFreePort(): Int = {
    val s = new ServerSocket(0)
    val port = s.getLocalPort
    // We have to release the port so the PostgreSQL server can use it. Note that there is a small
    // window for race, as releasing the port then handing it to the server is not atomic. If this
    // turns out to be an issue, we need to find an atomic way of doing that.
    s.close()
    port
  }

  private case class Tool private[Tool] (name: String) {

    import Tool._

    def path: Path = rlocation(binPath.resolve(name + binExtension))
  }

  private object Tool {
    private[Tool] val binPath = Paths.get("external/postgresql_dev_env/bin")
    private[Tool] val binExtension = if (isWindows) ".exe" else ""

    val createdb = Tool("createdb")
    val initdb = Tool("initdb")
    val pg_ctl = Tool("pg_ctl")
  }

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  private class ProcessFailedException(
      description: String,
      command: Seq[String],
      stdout: Option[String],
      stderr: Option[String],
      logs: Seq[String],
      cause: Throwable,
  ) extends RuntimeException(
        Seq(
          Some(s"Failed to $description."),
          Some(s"Command:"),
          Some(command.mkString("\n")),
          stdout.map(output => s"\nSTDOUT:\n$output"),
          stderr.map(output => s"\nSTDERR:\n$output"),
          Some(logs).filter(_.nonEmpty).map(lines => s"\nLogs:\n${lines.mkString("\n")}"),
        ).flatten.mkString("\n"),
        cause
      ) {
    def this(
        description: String,
        command: Seq[String],
        stdout: Option[String],
        stderr: Option[String],
        logs: Seq[String],
    ) = this(description, command, stdout, stderr, logs, null)
  }
}
