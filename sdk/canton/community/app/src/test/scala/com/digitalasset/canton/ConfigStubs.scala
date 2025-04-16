// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{AdminServerConfig, CryptoConfig, StorageConfig}
import com.digitalasset.canton.participant.config.{ParticipantInitConfig, ParticipantNodeConfig}
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig

/** Utilities for creating config objects for tests
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
object ConfigStubs {
  def participant: ParticipantNodeConfig =
    ParticipantNodeConfig(
      ParticipantInitConfig(),
      CryptoConfig(),
      null,
      None,
      adminApi,
      StorageConfig.Memory(),
    )

  def sequencer: SequencerNodeConfig =
    SequencerNodeConfig(adminApi = adminApi, publicApi = publicApi)

  def mediator: MediatorNodeConfig =
    MediatorNodeConfig(adminApi = adminApi)

  def adminApi: AdminServerConfig =
    AdminServerConfig(internalPort = Port.tryCreate(42).some)

  def publicApi: PublicServerConfig =
    PublicServerConfig(internalPort = Port.tryCreate(42).some)
}
