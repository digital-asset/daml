// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.TransactionFilter
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.value.{Identifier => ProtoIdentifier}
import com.daml.platform.Party
import com.daml.platform.api.v1.event.EventOps.EventOps

private[platform] object EventFilter {

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

}
