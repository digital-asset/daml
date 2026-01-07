// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}

trait CreateContinuityDumpsShard {
  def numShards: Int
  def shard: Int
  def supportedPVS: List[ProtocolVersion] =
    SplitCreateDataContinuityDumps.splitVersionsByShard(shard, numShards)
}

abstract class CreateBasicDataContinuityDumpsPostgres(override val shard: Int)
    extends CreateBasicDataContinuityDumps
    with CreateContinuityDumpsShard
    with DataContinuityTestFixturePostgres {
  override def numShards: Int = 3
  registerPlugin(plugin)
}

class CreateBasicDataContinuityDumpsPostgres_0
    extends CreateBasicDataContinuityDumpsPostgres(shard = 0)
class CreateBasicDataContinuityDumpsPostgres_1
    extends CreateBasicDataContinuityDumpsPostgres(shard = 1)
class CreateBasicDataContinuityDumpsPostgres_2
    extends CreateBasicDataContinuityDumpsPostgres(shard = 2)

// Synchronizer

abstract class CreateSynchronizerChangeDataContinuityDumpsPostgres(override val shard: Int)
    extends CreateSynchronizerChangeDataContinuityDumps
    with CreateContinuityDumpsShard
    with DataContinuityTestFixturePostgres {
  override def numShards: Int = 3
  registerPlugin(plugin)
}
class CreateSynchronizerChangeDataContinuityDumpsPostgres_0
    extends CreateSynchronizerChangeDataContinuityDumpsPostgres(shard = 0)
class CreateSynchronizerChangeDataContinuityDumpsPostgres_1
    extends CreateSynchronizerChangeDataContinuityDumpsPostgres(shard = 1)
class CreateSynchronizerChangeDataContinuityDumpsPostgres_2
    extends CreateSynchronizerChangeDataContinuityDumpsPostgres(shard = 2)

object SplitCreateDataContinuityDumps {

  def splitVersionsByShard(shard: Int, numShards: Int): List[ProtocolVersion] = {
    require(shard >= 0 && shard < numShards)
    allSupportedStablePv.toList.zipWithIndex
      .filter { case (_, index) => index % numShards == shard }
      .map(_._1)
  }

  lazy val allSupportedStablePv =
    ProtocolVersionCompatibility.supportedProtocols(
      includeAlphaVersions = false,
      includeBetaVersions = true,
      release = ReleaseVersion.current,
    )

}

// If you want to run locally, uncomment these

class CreateBasicDataContinuityDumpsPostgres_all
    extends CreateBasicDataContinuityDumps
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def supportedPVS = SplitCreateDataContinuityDumps.allSupportedStablePv
}

class CreateSynchronizerChangeDataContinuityDumpsPostgres_all
    extends CreateSynchronizerChangeDataContinuityDumps
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def supportedPVS = SplitCreateDataContinuityDumps.allSupportedStablePv
}
