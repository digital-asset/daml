// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.ledger.api.IdentityProviderId
import com.digitalasset.canton.ledger.localstore.api.PartyRecordStore
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.Future

class PartyRecordsExist(partyRecordStore: PartyRecordStore) {

  def filterPartiesExistingInPartyRecordStore(id: IdentityProviderId, parties: Set[Ref.Party])(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Set[Ref.Party]] =
    partyRecordStore.filterExistingParties(parties, id)

  def filterPartiesExistingInPartyRecordStore(parties: Set[Ref.Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Set[Ref.Party]] =
    partyRecordStore.filterExistingParties(parties)

}
