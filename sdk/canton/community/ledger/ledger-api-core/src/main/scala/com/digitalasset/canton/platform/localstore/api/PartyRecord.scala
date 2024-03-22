// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore.api

import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, ObjectMeta}

final case class PartyRecord(
    party: Ref.Party,
    metadata: ObjectMeta,
    identityProviderId: IdentityProviderId,
)
