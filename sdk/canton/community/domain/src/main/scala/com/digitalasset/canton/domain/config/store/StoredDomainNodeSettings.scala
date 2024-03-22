// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import com.digitalasset.canton.protocol.StaticDomainParameters

final case class StoredDomainNodeSettings(staticDomainParameters: StaticDomainParameters)
