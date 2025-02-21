// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.daml.lf.data.Ref

/** Represents a party with additional known information.
  *
  * @param party
  *   The stable unique identifier of a Daml party.
  * @param isLocal
  *   True if party is hosted by the backing participant.
  */
final case class IndexerPartyDetails(
    party: Ref.Party,
    isLocal: Boolean,
)
