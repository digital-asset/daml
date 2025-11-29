// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import cats.data.NonEmptyList
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.tests.manual.S3Synchronization.ContinuityDumpRef
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.nowarn

class BasicDataContinuityTestPostgres_1
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories(testAllPatchReleases)).first
}

class BasicDataContinuityTestPostgres_2
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories(testAllPatchReleases)).second
}

class BasicDataContinuityTestPostgres_3
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories(testAllPatchReleases)).third
}

class BasicDataContinuityTestPostgres_4
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories(testAllPatchReleases)).fourth
}

class SynchronizerChangeDataContinuityTestPostgres_1
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories(testAllPatchReleases)).first
}

class SynchronizerChangeDataContinuityTestPostgres_2
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories(testAllPatchReleases)).second
}

class SynchronizerChangeDataContinuityTestPostgres_3
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories(testAllPatchReleases)).third
}

class SynchronizerChangeDataContinuityTestPostgres_4
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories(testAllPatchReleases)).fourth
}

// If you want locally to run the test on all the folders, uncomment this class
class BasicDataContinuityTestPostgres_all
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    // In order to load dumps produced locally by a previous local run of
    // CreateBasicDataContinuityDumpsPostgres_all, replace S3Dump with LocalDump.
    S3Dump.getDumpDirectories(testAllPatchReleases)
}
class SynchronizerChangeDataContinuityTestPostgres_all
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    // In order to load dumps produced locally by a previous local run of
    // CreateSynchronizerChangeDataContinuityDumpsPostgres_all, replace S3Dump with LocalDump.
    S3Dump.getDumpDirectories(testAllPatchReleases)
}

object SplitReleaseVersion {

  private val numberOfClasses = PositiveInt.tryCreate(4)

  final case class Split(
      first: List[(ContinuityDumpRef, ProtocolVersion)],
      second: List[(ContinuityDumpRef, ProtocolVersion)],
      third: List[(ContinuityDumpRef, ProtocolVersion)],
      fourth: List[(ContinuityDumpRef, ProtocolVersion)],
  )

  @nowarn("msg=match may not be exhaustive")
  def split(allDumpDirectories: List[(ContinuityDumpRef, ProtocolVersion)]): Split = {
    val List(first, second, third, fourth) =
      ReleaseUtils.shard(NonEmptyList.fromListUnsafe(allDumpDirectories), numberOfClasses)
    Split(first, second, third, fourth)
  }
}
