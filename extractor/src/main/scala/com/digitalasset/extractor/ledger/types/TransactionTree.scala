// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.ledger.types

import com.daml.ledger.api.{v1 => api}
import com.daml.api.util.TimestampConversion
import com.google.protobuf.timestamp.{Timestamp => PTimeStamp}
import Event._
import java.time.Instant

import scalaz._
import Scalaz._
import api.transaction.TreeEvent.Kind

final case class TransactionTree(
    transactionId: String,
    workflowId: String,
    effectiveAt: Instant,
    offset: String,
    events: Map[String, Event],
    rootEventIds: Set[String]
)

object TransactionTree {
  private val effectiveAtLens =
    ReqFieldLens.create[api.transaction.TransactionTree, PTimeStamp]('effectiveAt)

  final implicit class ApiTransactionOps(val apiTransaction: api.transaction.TransactionTree)
      extends AnyVal {
    def convert: String \/ TransactionTree =
      for {
        apiEffectiveAt <- effectiveAtLens(apiTransaction)
        effectiveAt = TimestampConversion.toInstant(apiEffectiveAt)
        events <- apiTransaction.eventsById.toList.traverseU(kv =>
          kv._2.kind.convert.map(kv._1 -> _))
      } yield
        TransactionTree(
          apiTransaction.transactionId,
          apiTransaction.workflowId,
          effectiveAt,
          apiTransaction.offset,
          events.toMap,
          apiTransaction.rootEventIds.toSet
        )
  }

  final implicit class TreeEventKindOps(val kind: api.transaction.TreeEvent.Kind) extends AnyVal {
    def convert: String \/ Event = kind match {
      case Kind.Created(event) => event.convert
      case Kind.Exercised(event) => event.convert
      case Kind.Empty => "Unexpected `Empty` event.".left
    }
  }
}
