// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore.api

import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, ObjectMeta}
import com.digitalasset.daml.lf.data.Ref

final case class PartyRecord(
    party: Ref.Party,
    metadata: ObjectMeta,
    identityProviderId: IdentityProviderId,
) extends {
  override def toString: String =
    s"PartyRecord(party=$party, metadata=${metadata.toString.take(500)}, identityProviderId=$identityProviderId)"
}
