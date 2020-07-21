// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.ledger.api.domain.{CommandId, EventId, LedgerOffset, TransactionId}
import net.logstash.logback.argument.StructuredArguments
import scalaz.syntax.tag.ToTagOps

package object logging {

  private[services] def parties(parties: Iterable[String]): (String, String) =
    "parties" -> StructuredArguments.toString(parties.toArray)
  private[services] def party(party: String): (String, String) =
    "parties" -> StructuredArguments.toString(Array(party))
  private[services] def startExclusive(o: LedgerOffset): (String, String) =
    "startExclusive" -> offsetValue(o)
  private[services] def endInclusive(o: Option[LedgerOffset]): (String, String) =
    "endInclusive" -> nullableOffsetValue(o)
  private[services] def offset(o: Option[LedgerOffset]): (String, String) =
    "offset" -> nullableOffsetValue(o)
  private[this] def nullableOffsetValue(o: Option[LedgerOffset]): String =
    o.fold("")(offsetValue)
  private[this] def offsetValue(o: LedgerOffset): String =
    o match {
      case LedgerOffset.Absolute(absolute) => absolute
      case LedgerOffset.LedgerBegin => "%begin%"
      case LedgerOffset.LedgerEnd => "%end%"
    }
  private[services] def commandId(id: String): (String, String) =
    "commandId" -> id
  private[services] def commandId(id: CommandId): (String, String) =
    "commandId" -> id.unwrap
  private[services] def eventId(id: EventId): (String, String) =
    "eventId" -> id.unwrap
  private[services] def transactionId(id: TransactionId): (String, String) =
    "transactionId" -> id.unwrap

}
