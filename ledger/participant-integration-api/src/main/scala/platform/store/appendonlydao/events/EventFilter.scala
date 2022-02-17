// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.TransactionFilter
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.value.{Identifier => ProtoIdentifier}
import com.daml.platform.store.Contract.ActiveContract
import com.daml.platform.api.v1.event.EventOps.EventOps

private[platform] object EventFilter {

  // TODO Remove all usages of domain objects
  private def toLfIdentifier(id: ProtoIdentifier): Ref.Identifier =
    Ref.Identifier(
      packageId = Ref.PackageId.assertFromString(id.packageId),
      qualifiedName = Ref.QualifiedName(
        module = Ref.ModuleName.assertFromString(id.moduleName),
        name = Ref.DottedName.assertFromString(id.entityName),
      ),
    )

  def apply(event: Event)(txf: TransactionFilter): Option[Event] =
    Some(
      event.modifyWitnessParties(
        _.filter(party => txf(Party.assertFromString(party), toLfIdentifier(event.templateId)))
      )
    )
      .filter(_.witnessParties.nonEmpty)

  def apply(event: ActiveContract)(txf: TransactionFilter): Option[ActiveContract] =
    Some(event)
      .filter(ac =>
        (ac.signatories union ac.observers).exists(party =>
          txf(party, event.contract.unversioned.template)
        )
      )
      .map(
        _.copy(witnesses =
          event.witnesses.filter(party => txf(party, event.contract.unversioned.template))
        )
      )

}
