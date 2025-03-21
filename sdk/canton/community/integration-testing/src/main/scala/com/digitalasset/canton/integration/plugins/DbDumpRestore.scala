// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.{TempDirectory, TempFile}

import java.nio.file.Path
import scala.concurrent.{Future, blocking}
import scala.sys.process.*

trait DbDumpRestore {
  def listFiles(directory: File): List[File]
  def copy(src: File, tempDirectory: TempDirectory): Unit

  def copyToLocal(source: TempDirectory, target: File): Unit

  def saveDump(node: InstanceReference, tempFile: TempFile)(implicit
      env: TestConsoleEnvironment
  ): Future[Unit]

  def saveDump(nodeName: String, tempFile: TempFile): Future[Unit]

  def createParent(tempFile: TempFile): Future[Unit]

  def restoreDump(node: InstanceReference, dumpFileName: Path)(implicit
      env: TestConsoleEnvironment
  ): Future[Unit]

  def restoreDump(nodeName: String, dumpFileName: Path): Future[Unit]
}

trait DockerDbDumpRestore extends DbDumpRestore {
  def containerID: String

  def listFiles(directory: File): List[File] = {
    val remotePath = directory.path

    if (createAndRunDockerCommand(s"test -d $remotePath") == 1) // directory does not exist
      Nil
    else {
      val result = blocking(createDockerCommand(s"ls $remotePath").!!)
      result.split("\n").toList.map(File(remotePath) / _)
    }
  }

  def copy(src: File, tempDirectory: TempDirectory): Unit = {
    createAndRunDockerCommand(s"mkdir -p ${tempDirectory.directory}")
    s"docker cp $src $containerID:${tempDirectory.directory}".!
  }

  def copyToLocal(source: TempDirectory, target: File): Unit = {
    /*
      Without the trailing /. the behavior depends on whether the target
      directory exists: https://docs.docker.com/engine/reference/commandline/cp/
     */
    val sourcePath = source.directory.toString() + "/."

    s"mkdir -p ${source.path.getParent}".!
    s"docker cp $containerID:$sourcePath $target".!
  }

  protected def createAndRunDockerCommand(
      cmd: String,
      envVars: Map[String, String] = Map.empty,
  ): Int = blocking(createDockerCommand(cmd, envVars).!)

  protected def createDockerCommand(
      cmd: String,
      envVars: Map[String, String] = Map.empty,
      asUser: Option[String] = None,
  ): String = {
    val dockerEnvVars = envVars.map { case (name, value) => s"--env $name=$value" }.mkString(" ")
    val dockerAsUser = asUser.map(user => s"-u $user").getOrElse("")

    s"""docker exec $dockerAsUser $containerID bash -c "$cmd" $dockerEnvVars"""
  }
}

object DockerDbDumpRestore {
  val remoteTempBasePath: Path = File("/tmp/").path
}
