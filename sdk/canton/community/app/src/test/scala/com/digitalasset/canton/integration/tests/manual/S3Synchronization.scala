// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import better.files.File
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.tests.manual.DataContinuityTest.baseDbDumpPath
import com.digitalasset.canton.integration.tests.manual.S3Synchronization.{
  ContinuityDumpLocalRef,
  ContinuityDumpRef,
  ContinuityDumpS3Ref,
}
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import java.nio.file.Files
import scala.concurrent.blocking
import scala.jdk.CollectionConverters.*
import scala.sys.process.*

trait S3Synchronization { self: BaseTest =>

  /** A source of data continuity dumps: S3 or local.
    *
    * The local dump source is only useful for end-to-end local debugging of the creation of dumps
    * followed by their consumption on the same machine. S3 is the default source used by the tests
    * run by the CI.
    */
  sealed trait DumpSource {
    def listPvDirectories(): List[String]
    def mkContinuityDumpRef(path: String): ContinuityDumpRef

    private def releaseDumpDirectories(
        filterVersionMajorMinor: (Int, Int)
    ): List[(ReleaseVersion, ContinuityDumpRef)] = {
      val listing = listPvDirectories()
        .filterNot(_.matches(s".*-ad-hoc.*"))

      def getReleaseVersion(directory: String): ReleaseVersion =
        ReleaseVersion.tryCreate(directory.split("/").head)

      val latestDumpReleaseVersion = listing
        .map(getReleaseVersion)
        .filter(v => v.majorMinor == filterVersionMajorMinor)
        .max

      listing
        .map(directory =>
          (
            getReleaseVersion(directory),
            mkContinuityDumpRef(directory),
          )
        )
        .filter { case (version, _) =>
          // During the early main net Canton releases we only test data continuity against the latest snapshot of the release-line
          // TODO(#16458): Once back to stable releases, select multiple older releases for testing, e.g. 2.3,2.4,2.5 -> 2.6
          version == latestDumpReleaseVersion
        }
    }

    def getDumpDirectories(
        testAllPatchReleases: Boolean,
        includeDeletedPV: Boolean = false,
        filterVersionMajorMinor: (Int, Int) = ReleaseVersion.current.majorMinor,
    ): List[(ContinuityDumpRef, ProtocolVersion)] = {
      val testedReleaseDirectories =
        if (testAllPatchReleases) releaseDumpDirectories(filterVersionMajorMinor)
        else
          releaseDumpDirectories(filterVersionMajorMinor).collect {
            case (rv, file)
                if ReleaseUtils.reducedScopeOfPreviousSupportedStableReleases.exists(
                  _.releaseVersion == rv
                ) =>
              (rv, file)
          }

      val dumps: List[(ContinuityDumpRef, ProtocolVersion)] = testedReleaseDirectories.flatMap {
        case (_, releaseDirectory) =>
          val prefix = releaseDirectory.path
          prefix.split("/").last match {
            case file if file.startsWith(DataContinuityTest.protocolVersionPrefix) =>
              val rawPv = file
                .substring(DataContinuityTest.protocolVersionPrefix.length)
                .toInt

              val pv = ProtocolVersion
                .fromProtoPrimitive(rawPv, allowDeleted = includeDeletedPV)
                .valueOrFail(s"Unsupported protocol version $rawPv")

              if (pv.isDeleted && !includeDeletedPV)
                Nil
              else
                List((releaseDirectory, pv))
            case file =>
              logger.warn(s"""
                             |This directory's name $file doesn't start with ${DataContinuityTest.protocolVersionPrefix}
                             |This directory is ignored for data continuity tests""".stripMargin)
              Nil
          }
      }

      dumps
    }
  }

  case object S3Dump extends DumpSource {
    override def listPvDirectories(): List[String] =
      ("""
         |aws s3api list-objects --bucket canton-public-releases --prefix data-continuity-dumps/ --query 'CommonPrefixes[].{Prefix: Prefix}' --output text --no-sign-request --delimiter "/0"
         |""".stripMargin.!!).split("\n").toList
        .filter(_.contains("data-continuity-dumps"))
        .map(_.replace("data-continuity-dumps/", "").replace("/0", ""))

    override def mkContinuityDumpRef(path: String): ContinuityDumpRef =
      ContinuityDumpS3Ref(path)
  }

  case object LocalDump extends DumpSource {
    override def listPvDirectories(): List[String] = Files
      .walk(baseDbDumpPath.path)
      .iterator()
      .asScala
      .filter(Files.isDirectory(_))
      .map(baseDbDumpPath.path.relativize(_).toString)
      .filter(path => path.matches(".*/pv=\\d+$"))
      .toList

    override def mkContinuityDumpRef(path: String): ContinuityDumpRef =
      ContinuityDumpLocalRef(path)
  }
}

object S3Synchronization {
  sealed trait ContinuityDumpRef {
    val path: String
    def localDownloadPath: File
  }

  /** This class incapsulates the path to a data continuity dump in S3 bucket under
    * s3://canton-public-releases/data-continuity-dumps/ and provides a utility to download the dump
    * to a local path under `baseDbDumpPath`.
    * @param path
    *   The path to the dump under the shared prefix in the S3 bucket
    */
  final case class ContinuityDumpS3Ref(override val path: String) extends ContinuityDumpRef {
    lazy val localDownloadPath: File = {
      val syncCommand =
        s"aws s3 sync s3://canton-public-releases/data-continuity-dumps/$path ${baseDbDumpPath.path}/$path --no-sign-request"

      val syncResult = runSynchronized(syncCommand)
      if (syncResult != 0) {
        throw new RuntimeException(
          s"Failed to sync $path to $baseDbDumpPath: command '$syncCommand' exited with code $syncResult"
        )
      } else {
        (baseDbDumpPath / path).directory
      }
    }
  }

  /** This class is a pass-through for files in local data continuity dumps. It assumes that the
    * referenced file is already present on the disk and returns its path.
    */
  final case class ContinuityDumpLocalRef(override val path: String) extends ContinuityDumpRef {
    lazy val localDownloadPath: File =
      (baseDbDumpPath / path).directory
  }

  // aws S3 sync commands don't have any locks so concurrent runs will interfere with each other
  private def runSynchronized(command: String): Int =
    blocking {
      synchronized {
        command.!
      }
    }
}
