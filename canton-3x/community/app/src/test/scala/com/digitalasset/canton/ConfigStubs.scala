// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{
  CommunityAdminServerConfig,
  CommunityCryptoConfig,
  CommunityStorageConfig,
}
import com.digitalasset.canton.domain.config.{CommunityDomainConfig, DomainInitConfig}
import com.digitalasset.canton.domain.mediator.CommunityMediatorNodeXConfig
import com.digitalasset.canton.domain.sequencing.config.CommunitySequencerNodeXConfig
import com.digitalasset.canton.participant.config.{
  CommunityParticipantConfig,
  ParticipantInitConfig,
}

/** Utilities for creating config objects for tests
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
object ConfigStubs {
  def participant: CommunityParticipantConfig =
    CommunityParticipantConfig(
      ParticipantInitConfig(),
      CommunityCryptoConfig(),
      null,
      None,
      adminApi,
      CommunityStorageConfig.Memory(),
    )

  def domain: CommunityDomainConfig =
    CommunityDomainConfig(
      DomainInitConfig(),
      false,
      null,
      null,
      CommunityStorageConfig.Memory(),
      CommunityCryptoConfig(),
    )

  def sequencerx: CommunitySequencerNodeXConfig =
    CommunitySequencerNodeXConfig()

  def mediatorx: CommunityMediatorNodeXConfig =
    CommunityMediatorNodeXConfig()

  def adminApi: CommunityAdminServerConfig =
    CommunityAdminServerConfig(internalPort = Port.tryCreate(42).some)
}
