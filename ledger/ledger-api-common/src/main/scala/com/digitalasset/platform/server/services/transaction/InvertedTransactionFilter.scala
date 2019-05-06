// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.ledger.api.domain.TransactionFilter

import scala.collection.{breakOut, immutable, mutable}

/**
  * Exposes an inverted view of a transactionFilter.
  * Can be collapsed into a reverse lookup table.
  * See the purpose of the type params in the transform method.
  */
private final case class InvertedTransactionFilter[I, P](
    specificSubscriptions: immutable.Map[I, Set[P]],
    globalSubscribers: immutable.Set[P]) {

  /**
    * The purpose of this method is to enable quick conversions between the ledger api domain layer and daml-lf types,
    * so that the collapsed Map may have the appropriate types.
    */
  def transform[NI, NP](transId: I => NI, transParty: P => NP): InvertedTransactionFilter[NI, NP] =
    InvertedTransactionFilter[NI, NP](specificSubscriptions.map {
      case (k, v) => (transId(k), v.map(transParty))
    }, globalSubscribers.map(transParty))

}
private object InvertedTransactionFilter {
  val empty: InvertedTransactionFilter[DefinitionRef, Party] =
    InvertedTransactionFilter(Map.empty, Set.empty)

  private type TF = InvertedTransactionFilter[DefinitionRef, Party]

  // implementation is way simpler with mutable collections
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def extractFrom(
      transactionFilter: TransactionFilter): InvertedTransactionFilter[DefinitionRef, Party] = {
    val specific = new mutable.HashMap[DefinitionRef, mutable.Set[Party]]()
    with mutable.MultiMap[DefinitionRef, Party]
    val global = mutable.Set[Party]()

    transactionFilter.filtersByParty.foreach {
      case (party, filters) =>
        filters.inclusive match {
          case None => global += party
          case Some(inclusive) =>
            inclusive.templateIds.foreach { templateId =>
              specific.addBinding(templateId, party)
            }
        }
    }

    InvertedTransactionFilter(specific.map {
      case (k, v) => (k, v.toSet)
    }(breakOut), global.toSet)
  }
}
