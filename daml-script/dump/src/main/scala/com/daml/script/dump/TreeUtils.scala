// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.transaction.TreeEvent.Kind
import com.daml.ledger.api.v1.value.{Identifier, Value}
import com.daml.ledger.api.v1.value.Value.Sum
import scalaz.std.option._
import scalaz.std.list._
import scalaz.std.set._
import scalaz.syntax.foldable._

object TreeUtils {
  final case class Selector(i: Int)

  def traverseTree(tree: TransactionTree)(f: (List[Selector], TreeEvent.Kind) => Unit): Unit = {
    def traverseEv(ev: TreeEvent.Kind, f: (List[Selector], TreeEvent.Kind) => Unit): Unit =
      ev match {
        case Kind.Empty =>
        case created @ Kind.Created(_) =>
          f(List(), created)
        case exercised @ Kind.Exercised(value) =>
          f(List(), exercised)
          value.childEventIds.map(x => tree.eventsById(x).kind).zipWithIndex.foreach {
            case (ev, i) =>
              traverseEv(ev, { case (path, ev) => f(Selector(i) :: path, ev) })
          }
      }
    tree.rootEventIds.map(tree.eventsById(_)).zipWithIndex.foreach { case (ev, i) =>
      traverseEv(ev.kind, { case (path, ev) => f(Selector(i) :: path, ev) })
    }
  }

  def partiesInTree(tree: TransactionTree): Set[String] = {
    var parties: Set[String] = Set()
    traverseTree(tree) { case (_, ev) =>
      ev match {
        case Kind.Empty =>
        case Kind.Created(value) =>
          parties = parties.union(valueParties(Value.Sum.Record(value.getCreateArguments)))
        case Kind.Exercised(value) =>
          parties = parties.union(valueParties(value.getChoiceArgument.sum))
      }
    }
    parties
  }

  private def valueParties(v: Value.Sum): Set[String] = v match {
    case Sum.Empty => Set()
    case Sum.Record(value) =>
      value.fields.map(v => valueParties(v.getValue.sum)).foldLeft(Set[String]()) { case (x, xs) =>
        x.union(xs)
      }
    case Sum.Variant(value) => valueParties(value.getValue.sum)
    case Sum.ContractId(_) => Set()
    case Sum.List(value) =>
      value.elements.map(v => valueParties(v.sum)).foldLeft(Set[String]()) { case (x, xs) =>
        x.union(xs)
      }
    case Sum.Int64(_) => Set()
    case Sum.Numeric(_) => Set()
    case Sum.Text(_) => Set()
    case Sum.Timestamp(_) => Set()
    case Sum.Party(value) => Set(value)
    case Sum.Bool(_) => Set()
    case Sum.Unit(_) => Set()
    case Sum.Date(_) => Set()
    case Sum.Optional(value) => value.value.fold(Set[String]())(v => valueParties(v.sum))
    case Sum.Map(value) =>
      value.entries.map(e => valueParties(e.getValue.sum)).foldLeft(Set[String]()) { case (x, xs) =>
        x.union(xs)
      }
    case Sum.Enum(_) => Set[String]()
    case Sum.GenMap(value) =>
      value.entries.map(e => valueParties(e.getValue.sum)).foldLeft(Set[String]()) { case (x, xs) =>
        x.union(xs)
      }
  }

  case class CreatedContract(cid: String, tplId: Identifier, path: List[Selector])

  def treeCids(tree: TransactionTree): Seq[CreatedContract] = {
    var cids: Seq[CreatedContract] = Seq()
    traverseTree(tree) { case (selectors, kind) =>
      kind match {
        case Kind.Empty =>
        case Kind.Exercised(_) =>
        case Kind.Created(value) =>
          cids ++= Seq(CreatedContract(value.contractId, value.getTemplateId, selectors))
      }
    }
    cids
  }

  def evParties(ev: TreeEvent.Kind): Seq[String] = ev match {
    case TreeEvent.Kind.Created(create) => create.signatories
    case TreeEvent.Kind.Exercised(exercised) => exercised.actingParties
    case TreeEvent.Kind.Empty => Seq()
  }

  def treeRefs(t: TransactionTree): Set[Identifier] =
    t.eventsById.values.toList.foldMap(e => evRefs(e.kind))

  def evRefs(e: TreeEvent.Kind): Set[Identifier] = e match {
    case Kind.Empty => Set()
    case Kind.Created(value) => valueRefs(Sum.Record(value.getCreateArguments))
    case Kind.Exercised(value) => valueRefs(value.getChoiceArgument.sum)
  }

  def valueRefs(v: Value.Sum): Set[Identifier] = v match {
    case Sum.Empty => Set()
    case Sum.Record(value) =>
      Set(value.getRecordId).union(value.fields.toList.foldMap(f => valueRefs(f.getValue.sum)))
    case Sum.Variant(value) => Set(value.getVariantId).union(valueRefs(value.getValue.sum))
    case Sum.ContractId(_) => Set()
    case Sum.List(value) => value.elements.toList.foldMap(v => valueRefs(v.sum))
    case Sum.Int64(_) => Set()
    case Sum.Numeric(_) => Set()
    case Sum.Text(_) => Set()
    case Sum.Timestamp(_) => Set()
    case Sum.Party(_) => Set()
    case Sum.Bool(_) => Set()
    case Sum.Unit(_) => Set()
    case Sum.Date(_) => Set()
    case Sum.Optional(value) => value.value.foldMap(v => valueRefs(v.sum))
    case Sum.Map(value) => value.entries.toList.foldMap(e => valueRefs(e.getValue.sum))
    case Sum.Enum(value) => Set(value.getEnumId)
    case Sum.GenMap(value) =>
      value.entries.toList.foldMap(e => valueRefs(e.getKey.sum).union(valueRefs(e.getValue.sum)))
  }

}
