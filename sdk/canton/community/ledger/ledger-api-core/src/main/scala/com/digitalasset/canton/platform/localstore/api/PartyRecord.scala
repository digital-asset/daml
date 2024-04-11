// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore.api

import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, ObjectMeta}

final case class PartyRecord(
    party: Ref.Party,
    metadata: ObjectMeta,
    identityProviderId: IdentityProviderId,
) extends {
  override def toString: String =
    s"PartyRecord(party=$party, metadata=${metadata.toString.take(500)}, identityProviderId=$identityProviderId)"
}
