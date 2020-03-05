// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.platform.store.Contract.ActiveContract
import com.digitalasset.platform.api.v1.event.EventOps.EventOps

object EventFilter {

  // TODO Remove all usages of domain objects
  private def toLfIdentifier(id: Identifier): Ref.Identifier =
    Ref.Identifier(
      packageId = Ref.PackageId.assertFromString(id.packageId),
      qualifiedName = Ref.QualifiedName(
        module = Ref.ModuleName.assertFromString(id.moduleName),
        name = Ref.DottedName.assertFromString(id.entityName)
      )
    )

  def apply(event: Event)(txf: TransactionFilter): Option[Event] =
    Some(event.modifyWitnessParties(_.filter(party =>
      txf(Party.assertFromString(party), toLfIdentifier(event.templateId)))))
      .filter(_.witnessParties.nonEmpty)

  def apply(event: ActiveContract)(txf: TransactionFilter): Option[ActiveContract] =
    Some(event)
      .filter(ac =>
        (ac.signatories union ac.observers).exists(party => txf(party, event.contract.template)))
      .map(_.copy(witnesses = event.witnesses.filter(party => txf(party, event.contract.template))))

}
