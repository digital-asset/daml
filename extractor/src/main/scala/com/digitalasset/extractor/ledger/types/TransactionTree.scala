// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.extractor.helpers.TransactionTreeTrimmer

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

// This is the state _after_ applying template and party filtering.
// This means that this is no longer a valid subtransaction or tree.
// In particular, there can be events that are not reachable from any
// root node (e.g. because the root nodes were all for different templates)
// and the child event ids can point to nodes that have been filtered out.
final case class TransactionTree(
    transactionId: String,
    workflowId: String,
    effectiveAt: Instant,
    offset: String,
    // Ordered in pre-order (the order in which the events happen on the ledger).
    events: List[(String, Event)],
    rootEventIds: Seq[String],
)

object TransactionTree {
  private val effectiveAtLens =
    ReqFieldLens.create[api.transaction.TransactionTree, PTimeStamp](Symbol("effectiveAt"))

  final implicit class ApiTransactionOps(val apiTransaction: api.transaction.TransactionTree)
      extends AnyVal {
    private def filteredEvents(
        parties: Set[String],
        templateIds: Set[api.value.Identifier],
    ): List[(String, api.transaction.TreeEvent.Kind)] = {
      val events = ListBuffer.empty[(String, api.transaction.TreeEvent.Kind)]
      foreach {
        case (id, ev) if TransactionTreeTrimmer.shouldKeep(parties, templateIds)(ev) =>
          events += ((id, ev))
        case _ =>
      }
      events.result()
    }
    def convert(
        parties: Set[String],
        templateIds: Set[api.value.Identifier],
    ): String \/ TransactionTree =
      for {
        apiEffectiveAt <- effectiveAtLens(apiTransaction)
        effectiveAt = TimestampConversion.toInstant(apiEffectiveAt)
        events <- ApiTransactionOps(apiTransaction)
          .filteredEvents(parties, templateIds)
          .traverse(kv => kv._2.kind.convert.map(kv._1 -> _))
        kept = events.map(_._1).toSet
      } yield TransactionTree(
        apiTransaction.transactionId,
        apiTransaction.workflowId,
        effectiveAt,
        apiTransaction.offset,
        events,
        apiTransaction.rootEventIds.filter(kept),
      )
    // pre-order traversal over the transaction tree. This is the equivalent of
    // the traversal in com.daml.lf.transaction for the client side.
    private def foreach(f: (String, api.transaction.TreeEvent.Kind) => Unit): Unit = {
      @tailrec
      def go(toVisit: List[String]): Unit = toVisit match {
        case id :: toVisit =>
          apiTransaction.eventsById.get(id) match {
            case None =>
              throw new IllegalArgumentException(s"Missing event id in transaction tree: $id")
            case Some(node) =>
              f(id, node.kind)
              node.kind match {
                case Kind.Exercised(e) =>
                  go(List(e.childEventIds: _*) ++ toVisit)
                case Kind.Created(_) =>
                  go(toVisit)
                case Kind.Empty =>
              }
          }
        case Nil =>
      }
      go(List(apiTransaction.rootEventIds: _*))
    }
  }

  final implicit class TreeEventKindOps(val kind: api.transaction.TreeEvent.Kind) extends AnyVal {
    def convert: String \/ Event = (kind match {
      case Kind.Created(event) => event.convert
      case Kind.Exercised(event) => event.convert
      case Kind.Empty => "Unexpected `Empty` event.".left
    }).widen
  }
}
