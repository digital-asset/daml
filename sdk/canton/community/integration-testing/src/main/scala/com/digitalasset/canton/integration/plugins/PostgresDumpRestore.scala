// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.util.CommandRunner
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.db.PostgresTestContainerSetup
import com.digitalasset.canton.{TempDirectory, TempFile}

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.sys.process.*

/** The main purpose of this trait is to expose methods to dump and restore Postgres databases. The
  * `LocalPostgresDumpRestore` class uses locally available `pg_dump` and `pg_restore` while
  * `DockerPostgresDumpRestore` calls tools that run inside a docker container.
  *
  * Some other utility methods are exposed:
  *   - `listFiles` to list the content of a directory
  *   - `copy` to copy a local file/directory to the target (locally or inside the docker image)
  */
sealed trait PostgresDumpRestore extends DbDumpRestore {
  protected def port: Int
  protected def plugin: UsePostgres
  protected def dbBasicConfig: DbBasicConfig =
    plugin.dbSetup.basicConfig

  protected implicit def executionContext: ExecutionContext

  def saveDump(node: InstanceReference, tempFile: TempFile)(implicit
      env: TestConsoleEnvironment
  ): Future[Unit] = saveDump(node.name, tempFile)

  def saveDump(nodeName: String, tempFile: TempFile): Future[Unit] = for {
    _ <- createParent(tempFile)
    _ <- runDumpCommand(plugin.generateDbName(nodeName), tempFile.path)
  } yield ()

  def restoreDump(node: InstanceReference, dumpFileName: Path)(implicit
      env: TestConsoleEnvironment
  ): Future[Unit] = restoreDump(node.name, dumpFileName)

  def restoreDump(nodeName: String, dumpFileName: Path): Future[Unit] = {
    val dbName = plugin.generateDbName(nodeName)
    for {
      // somehow, pg_restore -c -C doesn't seem to cleanup the database, so doing it separately
      _ <- plugin
        .recreateDatabaseForNodeWithName(nodeName)
        .failOnShutdownToAbortException("restoreDump")
      _ <- runDumpOrRestoreCommand(
        "pg_restore",
        dbName,
        s"--no-privileges --no-owner --dbname=$dbName $dumpFileName",
      )
    } yield ()
  }

  protected def runDumpCommand(dbName: String, dumpFileName: Path): Future[Unit] =
    runDumpOrRestoreCommand("pg_dump", dbName, s"--format=c --file=$dumpFileName $dbName")

  protected def runDumpOrRestoreCommand(
      command: String,
      dbName: String,
      extraArgs: String,
  ): Future[Unit]

  protected def parametersAndEnvVariables(
      args: String,
      dbName: String,
  ): (String, Map[String, String]) = {
    val basicConfig = dbBasicConfig
    val dbUserName = s"$dbName-user"
    val dbPassword = "user"
    val host = basicConfig.host

    (
      s"-U $dbUserName -h $host -p $port -w $args",
      Map("PGPASSWORD" -> dbPassword),
    )
  }
}

final case class LocalPostgresDumpRestore(plugin: UsePostgres) extends PostgresDumpRestore {

  override protected implicit def executionContext: ExecutionContext =
    plugin.dbSetupExecutionContext

  protected def port: Int = dbBasicConfig.port

  def createParent(tempFile: TempFile): Future[Unit] = Future {
    s"mkdir -p ${tempFile.parentDirectory}".!
  }

  def listFiles(tempDirectory: File): List[File] =
    if (tempDirectory.exists)
      tempDirectory.list.toList
    else Nil

  def copy(src: File, tempDirectory: TempDirectory): Unit =
    LocalPostgresDumpRestore.copy(src, tempDirectory)

  // In the non-docker case, this is a noop
  def copyToLocal(source: TempDirectory, target: File): Unit = ()

  def saveDump(node: InstanceReference, dumpFileName: Path): Future[Unit] =
    runDumpCommand(plugin.generateDbName(node.name), dumpFileName)

  protected def runDumpOrRestoreCommand(
      command: String,
      dbName: String,
      extraArgs: String,
  ): Future[Unit] = {
    val runner = new CommandRunner(command)
    val (parameters, envVariables) = parametersAndEnvVariables(extraArgs, dbName)

    Future(runner.run(parameters, None, envVariables.toSeq))
  }
}

object LocalPostgresDumpRestore {
  // in rare cases, this lead to issues with concurrent copying because in the jdk 11 `UnixCopyFile` implementation,
  // `overwrite` means first deleting and then writing the file - thus this is not thread-safe if copying to the same
  // directory
  def copy(src: File, tempDirectory: TempDirectory): Unit = blocking(this.synchronized {
    if (!tempDirectory.directory.exists) tempDirectory.directory.createDirectoryIfNotExists()
    src.copyToDirectory(tempDirectory.directory)(copyOptions = File.CopyOptions(overwrite = true))
  })
}

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
final case class DockerPostgresDumpRestore(
    plugin: UsePostgres,
    containerID: String,
) extends PostgresDumpRestore
    with DockerDbDumpRestore {
  protected def port: Int = 5432

  override protected implicit def executionContext: ExecutionContext =
    plugin.dbSetupExecutionContext

  protected def runDumpOrRestoreCommand(
      command: String,
      dbName: String,
      extraArgs: String,
  ): Future[Unit] = {
    val (parameters, envVariables) = parametersAndEnvVariables(extraArgs, dbName)
    runBashCommandInDocker(s"$command $parameters", envVariables)
  }

  def createParent(tempFile: TempFile): Future[Unit] =
    runBashCommandInDocker(s"mkdir -p ${tempFile.parentDirectory} && touch $tempFile")

  private def runBashCommandInDocker(
      cmd: String,
      envVars: Map[String, String] = Map.empty,
  ): Future[Unit] = Future(createAndRunDockerCommand(cmd, envVars)).map(_ => ())
}

object PostgresDumpRestore {
  /* If `forceLocal` is true, then we will call the local tools regardless
   * of the fact that we have a TestContainer or not. This can be useful if
   * one wants to keep the dumps. */
  def apply(plugin: UsePostgres, forceLocal: Boolean): PostgresDumpRestore =
    plugin.dbSetup match {
      case container: PostgresTestContainerSetup if !forceLocal =>
        DockerPostgresDumpRestore(plugin, container.getContainerID)

      case _ => LocalPostgresDumpRestore(plugin)
    }
}
