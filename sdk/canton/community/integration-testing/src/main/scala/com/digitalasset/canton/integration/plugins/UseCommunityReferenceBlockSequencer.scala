// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.{
  SequencerSynchronizerGroups,
  SingleSynchronizer,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.CommunityReferenceSequencerDriverFactory

import scala.reflect.ClassTag

class UseCommunityReferenceBlockSequencer[S <: StorageConfig](
    override protected val loggerFactory: NamedLoggerFactory,
    sequencerGroups: SequencerSynchronizerGroups = SingleSynchronizer,
    postgres: Option[UsePostgres] = None,
)(implicit _c: ClassTag[S])
    extends UseReferenceBlockSequencerBase[
      S,
    ](loggerFactory, "reference", "community-reference", sequencerGroups, postgres) {

  override protected val driverFactory = new CommunityReferenceSequencerDriverFactory

}
