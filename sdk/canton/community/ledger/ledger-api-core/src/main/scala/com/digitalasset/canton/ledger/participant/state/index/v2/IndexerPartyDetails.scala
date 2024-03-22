// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.daml.lf.data.Ref

/** Represents a party with additional known information.
  *
  * @param party       The stable unique identifier of a Daml party.
  * @param displayName Human readable name associated with the party. Might not be unique. If defined must be a non-empty string.
  * @param isLocal     True if party is hosted by the backing participant.
  */
final case class IndexerPartyDetails(
    party: Ref.Party,
    displayName: Option[String],
    isLocal: Boolean,
)
