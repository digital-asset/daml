// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import com.digitalasset.canton.config.InitConfigBase

final case class DomainInitConfig(
    identity: Option[InitConfigBase.Identity] = Some(InitConfigBase.Identity()),
    domainParameters: DomainParametersConfig = DomainParametersConfig(),
) extends InitConfigBase
