// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.ObjectMeta
import com.daml.lf.data.Ref

final case class PartyRecord(
    party: Ref.Party,
    metadata: ObjectMeta,
    identityProviderId: Option[Ref.IdentityProviderId]
)
