// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import better.files.File
import cats.data.NonEmptyList
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}

import java.nio.file.{Files, Paths}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** A collection of small utilities for tests that have no obvious home */
@SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
object ReleaseUtils {
  final case class TestedRelease(
      releaseVersion: ReleaseVersion,
      protocolVersions: NonEmpty[List[ProtocolVersion]],
  )

  /** Given a list of [[TestedRelease]] returns a new list of [[TestedRelease]] where each element
    * is a release version together with one of its supported protocol version.
    *
    * Example:
    *   - Given [(2.7.9, [3, 4, 5]), (2.9.0, [5, 6])
    *   - Returns [(2.7.9, [3]), (2.7.9, [4]), (2.7.9, [5]), (2.9.0, [5]), (2.9.0, [6])]
    */
  def zipReleasesWithProtocolVersions(releases: List[TestedRelease]): List[TestedRelease] =
    releases.flatMap { case TestedRelease(v, ps) =>
      ps.map(e => TestedRelease(v, NonEmpty.mk(List, e)))
    }

  /** Given a list of `E` and a number of shards `n` returns a new list with exactly `n` sub-lists
    * (shards) containing ideally the same number of elements.
    *
    * For a given list having fewer elements than the given `n` the result is padded with empty
    * lists so that the resulting list has size `n`.
    *
    * Examples:
    *   - List(1, 2, 3) and n=2 => List(List(1), List(2, 3))
    *   - List(1, 2, 3) and n=4 => List(List(1), List(2), List(3), List())
    */
  def shard[E](list: NonEmptyList[E], numberOfShards: PositiveInt): List[List[E]] = {
    val items = list.toList
    val n = numberOfShards.value
    val numItems = items.size
    val sharded =
      if (numItems < n) items.grouped(1).padTo(n, Nil)
      else {
        val (itemsPerShard, remainingItems) = (numItems / n, numItems % n)
        val (left, right) = items.splitAt(numItems - remainingItems * (itemsPerShard + 1))
        left.grouped(itemsPerShard) ++ right.grouped(itemsPerShard + 1)
      }
    sharded.toList
  }

  private lazy val previousStableReleases: List[TestedRelease] =
    File("release-notes/")
      .list(file => file.name.startsWith(ReleaseVersion.current.major.toString))
      .map(_.name.replace(".md", ""))
      .map(ReleaseVersion.tryCreate)
      .collect {
        case releaseVersion if releaseVersion.isStable =>
          TestedRelease(
            releaseVersion,
            ProtocolVersionCompatibility.supportedProtocols(
              includeAlphaVersions = false,
              includeBetaVersions = true,
              release = releaseVersion,
            ),
          )
      }
      .toList
      .sortBy(_.releaseVersion)

  // All previous stable releases minus releases that support only deleted protocol versions
  lazy val previousSupportedStableReleases: List[TestedRelease] =
    previousStableReleases.mapFilter { case TestedRelease(releaseVersion, protocolVersions) =>
      NonEmpty
        .from(protocolVersions.filterNot(_.isDeleted))
        .map(TestedRelease(releaseVersion, _))
    }

  private def reduceToLatestSupportedStablesReleases(releases: List[TestedRelease]) =
    releases.sortBy(_.releaseVersion).foldLeft(List.empty[TestedRelease]) {
      case (one :: rest, item)
          // keep the latest of each patch version
          if (one.releaseVersion.majorMinorMatches(item.releaseVersion)) =>
        item :: rest
      case (acc, item) => item :: acc
    }

  /** Returns release versions for the latest patch and minor version together with their list of
    * supported protocol versions for a given range of release versions.
    *
    * Example:
    *   - Latest stable releases for 2.7 and 2.8 are 2.7.9 and 2.8.3
    *   - Given a range from 2.7.0 to 2.9.0 this function returns the list [(2.7.9, [3, 4, 5]),
    *     (2.8.3, [3, 4, 5])
    */
  def latestSupportedStableReleasesInRange(
      fromInclusive: ReleaseVersion,
      toExclusive: ReleaseVersion,
  ): List[TestedRelease] =
    reduceToLatestSupportedStablesReleases(previousStableReleases.filter {
      case TestedRelease(releaseVersion, _) =>
        releaseVersion >= fromInclusive && releaseVersion < toExclusive
    })

  /** The first time we attempt to get a release, a future is inserted into the map. This allows to
    * synchronize between different requests for the same release.
    */
  private val releasesRetrieval: TrieMap[ReleaseVersion, Future[String]] = TrieMap.empty
  private val lock = new Mutex()

  /** If the .tar.gz corresponding to release is not found locally, attempts to download it from
    * artifactory. Then, extract the .tar.gz file.
    * @param release
    *   Version that needs to be retrieved
    * @return
    *   Directory containing the downloaded release
    */
  @SuppressWarnings(Array("com.digitalasset.canton.SynchronizedFuture"))
  def retrieve(
      release: ReleaseVersion
  )(implicit elc: ErrorLoggingContext, ec: ExecutionContext): Future[String] =
    lock.exclusive {
      releasesRetrieval.get(release) match {
        case Some(releaseRetrieval) => releaseRetrieval
        case None =>
          val releaseRetrieval = Future(downloadAndExtract(release))
          releasesRetrieval.put(release, releaseRetrieval).discard
          releaseRetrieval
      }
    }

  /** This method should not be called concurrently for the same release. Use [[retrieve]] method
    * above instead.
    *
    * @param release
    *   Release version th
    * @return
    *   Directory containing the downloaded release
    */
  private def downloadAndExtract(
      release: ReleaseVersion
  )(implicit elc: ErrorLoggingContext): String = {
    import scala.sys.process.*

    val cantonDir = s"tmp/canton-community-$release/bin/canton"
    if (Files.exists(Paths.get(cantonDir))) {
      elc.info(s"Release $release already downloaded.")
      cantonDir
    } else {
      val processLogger = new BufferedProcessLogger
      elc.info(s"Beginning to download release $release. This may take a while. ")
      val exitCode = s"scripts/testing/get-release.sh $release".!(processLogger)
      ErrorUtil.requireArgument(
        exitCode == 0,
        s"getting release $release failed with exit code $exitCode. Download script output: \n ${processLogger.output()}",
      )
      elc.info(
        s"Finished downloading release $release. Download script output: \n ${processLogger.output()}"
      )

      cantonDir
    }
  }

}
