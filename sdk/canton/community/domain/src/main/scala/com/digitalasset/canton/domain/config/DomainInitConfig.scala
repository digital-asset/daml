// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import com.digitalasset.canton.config.InitConfigBase
import com.digitalasset.canton.version.DomainProtocolVersion

final case class DomainInitConfig(
    identity: Option[InitConfigBase.Identity] = Some(InitConfigBase.Identity()),
    domainParameters: DomainParametersConfig,
) extends InitConfigBase

object DomainInitConfig {
  def defaults(protocolVersion: DomainProtocolVersion): DomainInitConfig =
    DomainInitConfig(domainParameters = DomainParametersConfig.defaults(protocolVersion))
}
