// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import cats.syntax.either.*
import com.digitalasset.canton.config.{DbConfig, StorageConfig}
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.synchronizer.block.SequencerDriver
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.{
  CommunityReferenceSequencerDriverFactory,
  ReferenceSequencerDriver,
}
import com.digitalasset.canton.time.TimeProvider

import java.util.concurrent.atomic.AtomicReference

class ReferenceSequencerDriverApiConformanceTest
    extends ReferenceSequencerDriverApiConformanceTestBase[
      ReferenceSequencerDriver.Config[StorageConfig]
    ] {

  private val driverFactory = new CommunityReferenceSequencerDriverFactory

  override protected final val driverConfig
      : AtomicReference[Option[ReferenceSequencerDriver.Config[StorageConfig]]] =
    new AtomicReference(Some(ReferenceSequencerDriver.Config(StorageConfig.Memory())))

  override protected final lazy val plugin =
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory)

  override protected final def parseConfig(
      maybeConfig: Option[SequencerConfig]
  ): ReferenceSequencerDriver.Config[StorageConfig] =
    maybeConfig match {
      case Some(SequencerConfig.External(_, _, config)) =>
        driverFactory.configParser
          .from(config)
          .valueOr(e => sys.error(e.toString))
      case _ => sys.error("sequencer not defined")
    }

  override protected final def createDriver(
      timeProvider: TimeProvider,
      firstBlockHeight: Option[Long],
      topologyClient: SynchronizerCryptoClient,
  ): SequencerDriver =
    driverFactory.create(
      config = driverConfig
        .get()
        .getOrElse(sys.error("Driver config is not present")),
      nonStandardConfig = false,
      timeProvider,
      firstBlockHeight,
      synchronizerId.toString,
      loggerFactory,
    )
}
