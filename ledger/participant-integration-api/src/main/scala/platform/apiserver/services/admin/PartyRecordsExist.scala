// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.PartyRecordStore

import scala.concurrent.Future

class PartyRecordsExist(partyRecordStore: PartyRecordStore) {

  def filterPartiesExistingInPartyRecordStore(id: IdentityProviderId, parties: Set[Ref.Party])(
      implicit loggingContext: LoggingContext
  ): Future[Set[Ref.Party]] =
    partyRecordStore.filterExistingParties(parties, id)

}
