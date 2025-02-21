// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{AdminServerConfig, CryptoConfig, StorageConfig}
import com.digitalasset.canton.participant.config.{LocalParticipantConfig, ParticipantInitConfig}
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.mediator.CommunityMediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.config.CommunitySequencerNodeConfig

/** Utilities for creating config objects for tests
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
object ConfigStubs {
  def participant: LocalParticipantConfig =
    LocalParticipantConfig(
      ParticipantInitConfig(),
      CryptoConfig(),
      null,
      None,
      adminApi,
      StorageConfig.Memory(),
    )

  def sequencer: CommunitySequencerNodeConfig =
    CommunitySequencerNodeConfig(adminApi = adminApi, publicApi = publicApi)

  def mediator: CommunityMediatorNodeConfig =
    CommunityMediatorNodeConfig(adminApi = adminApi)

  def adminApi: AdminServerConfig =
    AdminServerConfig(internalPort = Port.tryCreate(42).some)

  def publicApi: PublicServerConfig =
    PublicServerConfig(internalPort = Port.tryCreate(42).some)
}
