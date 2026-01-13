// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import better.files.File
import com.digitalasset.canton.integration.tests.manual.DataContinuityTest.baseDbDumpPath
import com.digitalasset.canton.integration.tests.manual.S3Synchronization.{
  ContinuityDumpLocalRef,
  ContinuityDumpRef,
  ContinuityDumpS3Ref,
}
import com.digitalasset.canton.util.Mutex
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import com.digitalasset.canton.{FutureHelpers, TestEssentials}

import java.nio.file.Files
import scala.jdk.CollectionConverters.*
import scala.math.Ordering.Implicits.*
import scala.sys.process.*

trait S3Synchronization extends FutureHelpers with TestEssentials {

  /** A source of data continuity dumps: S3 or local.
    *
    * The local dump source is only useful for end-to-end local debugging of the creation of dumps
    * followed by their consumption on the same machine. S3 is the default source used by the tests
    * run by the CI.
    */
  sealed trait DumpSource {

    /** List all the dumps: one entry per (release, pv)
      */
    def listPvDirectories(): List[String]

    /** List all the dumps: one entry per release
      */
    def listVersionDirectories(): List[String] =
      listPvDirectories().map(_.takeWhile(_ != '/')).distinct

    def mkContinuityDumpRef(path: String): ContinuityDumpRef

    /** List the dumps on S3
      * @param majorUpgradeTestFrom
      *   If defined, tests major upgrade from the specified version
      * @param perProtocolVersion
      *   - If true, returns the reference to the sub-directory corresponding to the pv
      *   - If false, returns the reference to the top-level directory corresponding to the release
      *     version
      */
    private def releaseDumpDirectories(
        majorUpgradeTestFrom: Option[(Int, Int)],
        perProtocolVersion: Boolean,
    ): List[(ContinuityDumpRef, ReleaseVersion)] = {
      val onlyLatestDump = majorUpgradeTestFrom.isDefined
      val baseListing = if (perProtocolVersion) listPvDirectories() else listVersionDirectories()

      val listing = baseListing
        .filter(path =>
          !path.matches(s".*-ad-hoc.*") || path.contains("3.4.10-ad-hoc.20251211.17454.0.v04f84c64")
        )
        // Can be dropped when we have the new non-ad-hoc dumps
        .filterNot(_.contains("3.4.10-snapshot.20251209.17450.0.vfc6a5830"))

      def getReleaseVersion(directory: String): ReleaseVersion =
        ReleaseVersion.tryCreate(directory.split("/").head)

      val latestDumpReleaseVersion = listing
        .map(getReleaseVersion)
        .filter(v => majorUpgradeTestFrom.forall(_ == v.majorMinor))
        .max

      listing
        .map(directory =>
          (
            getReleaseVersion(directory),
            mkContinuityDumpRef(directory),
          )
        )
        // Data continuity started with 3.4
        .filter { case (version, _) => version.majorMinor >= (3, 4) }
        // Additional filtering
        .filter { case (version, _) => !onlyLatestDump || version == latestDumpReleaseVersion }
        /*
        We want to keep:
        - all stable releases
        - at most one snapshot: the one that is higher than the latest release. It allows to detect breaking changes early.
         */
        .sortBy { case (version, _) => version }(implicitly[Ordering[ReleaseVersion]].reverse)
        .zipWithIndex
        .collect { case ((version, ref), idx) if version.isStable || idx == 0 => (ref, version) }
    }

    /** Returns the list of dumps, one for each release version (discarding protocol versions)
      * @param majorUpgradeTestFrom
      *   If defined, tests major upgrade from the specified version
      */
    def getDumpBaseDirectoriesForVersion(
        majorUpgradeTestFrom: Option[(Int, Int)] = None
    ): List[(ContinuityDumpRef, ReleaseVersion)] =
      releaseDumpDirectories(majorUpgradeTestFrom, perProtocolVersion = false)

    /** Returns the list of dumps and protocol versions to be tested.
      * @param majorUpgradeTestFrom
      *   If defined, tests major upgrade from the specified version
      */
    def getDumpDirectories(
        majorUpgradeTestFrom: Option[(Int, Int)] = None
    ): List[(ContinuityDumpRef, ProtocolVersion)] = {
      val testedReleaseDirectories =
        releaseDumpDirectories(majorUpgradeTestFrom, perProtocolVersion = true)

      val dumps: List[(ContinuityDumpRef, ProtocolVersion)] = testedReleaseDirectories.flatMap {
        case (pvDirectory, _) =>
          val prefix = pvDirectory.path
          prefix.split("/").last match {
            case file if file.startsWith(DataContinuityTest.protocolVersionPrefix) =>
              val rawPv = file
                .substring(DataContinuityTest.protocolVersionPrefix.length)
                .toInt

              val pv = ProtocolVersion
                .fromProtoPrimitive(rawPv)
                .valueOrFail(s"Unsupported protocol version $rawPv")

              if (pv.isDeleted)
                Nil
              else
                List((pvDirectory, pv))
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

  /** This class encapsulates the path to a data continuity dump in S3 bucket under
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

  private val lock = new Mutex()
  // aws S3 sync commands don't have any locks so concurrent runs will interfere with each other
  private def runSynchronized(command: String): Int =
    lock.exclusive {
      command.!
    }
}
