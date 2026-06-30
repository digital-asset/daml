// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.ledger.client

import com.digitalasset.daml.lf.data.Ref

/** Minimal SDK-side replacement for the canton ledger API `PartyDetails`. */
final case class PartyDetails(
    party: Ref.Party,
    isLocal: Boolean,
    metadata: ObjectMeta,
    identityProviderId: IdentityProviderId,
)
