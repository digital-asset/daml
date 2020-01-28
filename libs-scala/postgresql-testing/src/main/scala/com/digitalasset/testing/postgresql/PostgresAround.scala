// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing.postgresql

import java.io.StringWriter
import java.net.{InetAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean

import com.digitalasset.testing.postgresql.PostgresAround._
import org.apache.commons.io.{FileUtils, IOUtils}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.Random
import scala.util.control.NonFatal

trait PostgresAround {
  @volatile
  protected var postgresFixture: PostgresFixture = _

  private val started: AtomicBoolean = new AtomicBoolean(false)

  protected def startEphemeralPostgres(): Unit = {
    logger.info("Starting an ephemeral PostgreSQL instance...")
    val tempDir = Files.createTempDirectory("postgres_test")
    val dataDir = tempDir.resolve("data")
    val confFile = Paths.get(dataDir.toString, "postgresql.conf")
    val port = findFreePort()
    val jdbcUrl = s"jdbc:postgresql://$hostName:$port/$databaseName?user=$userName"
    val logFile = Files.createFile(tempDir.resolve("postgresql.log"))
    postgresFixture = PostgresFixture(jdbcUrl, port, tempDir, dataDir, confFile, logFile)

    try {
      initializeDatabase()
      createConfigFile()
      startPostgres()
      createTestDatabase(databaseName)
      logger.info(s"PostgreSQL has started on port $port.")
    } catch {
      case NonFatal(e) =>
        stopPostgres()
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

  protected def startPostgres(): Unit = {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
        "Attempted to start PostgreSQL, but it has already been started.",
      )
    }
    try {
      run(
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
    } catch {
      case NonFatal(e) =>
        logger.error("Starting PostgreSQL failed.", e)
        started.set(false)
        throw e
    }
  }

  protected def stopPostgres(): Unit = {
    if (started.compareAndSet(true, false)) {
      logger.info("Stopping PostgreSQL...")
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
      logger.info("PostgreSQL has stopped.")
    }
  }

  protected def createNewDatabase(prefix: String = "test_"): PostgresFixture = {
    val newDatabaseName = s"$prefix${Random.nextInt(Int.MaxValue)}"
    createTestDatabase(newDatabaseName)
    val jdbcUrl =
      s"jdbc:postgresql://$hostName:${postgresFixture.port}/$newDatabaseName?user=$userName"
    postgresFixture.copy(jdbcUrl = jdbcUrl)
  }

  private def initializeDatabase(): Unit = run(
    "initialize the PostgreSQL database",
    Tool.initdb,
    s"--username=$userName",
    if (isWindows) "--locale=English_United States" else "--locale=en_US.UTF-8",
    "-E",
    "UNICODE",
    "-A",
    "trust",
    postgresFixture.dataDir.toString.replaceAllLiterally("\\", "/"),
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
          |listen_addresses = '$hostName'
          |port = ${postgresFixture.port}
        """.stripMargin
    Files.write(postgresFixture.confFile, configText.getBytes(StandardCharsets.UTF_8))
    ()
  }

  private def createTestDatabase(name: String): Unit = run(
    "create the database",
    Tool.createdb,
    "-h",
    hostName,
    "-U",
    userName,
    "-p",
    postgresFixture.port.toString,
    name,
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

  private val hostName = InetAddress.getLoopbackAddress.getHostName
  private val userName = "test"
  private val databaseName = "test"

  private def findFreePort(): Int = {
    val s = new ServerSocket(0)
    val port = s.getLocalPort
    // We have to release the port so the PostgreSQL server can use it. Note that there is a small
    // window for race, as releasing the port then handing it to the server is not atomic. If this
    // turns out to be an issue, we need to find an atomic way of doing that.
    s.close()
    port
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
        cause,
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
