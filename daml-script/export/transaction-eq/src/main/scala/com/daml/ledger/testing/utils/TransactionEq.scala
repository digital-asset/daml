// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testing.utils

import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v2.transaction.TransactionTree
import com.daml.ledger.api.v1.{value => v}
import com.digitalasset.canton.ledger.api.refinements.ApiTypes._

object TransactionEq {
  // Helper which constructs a temporary
  def equivalent(ts1: Seq[TransactionTree], ts2: Seq[TransactionTree]): Either[String, Unit] = {
    val comparator = new Comparator()
    if (ts1.length == ts2.length) {
      ts1.zip(ts2).toList.traverse_ { case (t1, t2) =>
        comparator.unify(t1, t2)
      }
    } else {
      Left(s"TransactionTree streams have different lengths: ${ts1.length} /= ${ts2.length}")
    }
  }

  class Comparator {
    var partiesLR: Map[Party, Party] = Map.empty
    var partiesRL: Map[Party, Party] = Map.empty
    var contractIdsLR: Map[ContractId, ContractId] = Map.empty
    var contractIdsRL: Map[ContractId, ContractId] = Map.empty
    def unify(t1: TransactionTree, t2: TransactionTree): Either[String, Unit] = {
      val rootNodes1 = t1.rootEventIds.length
      val rootNodes2 = t2.rootEventIds.length
      if (rootNodes1 != rootNodes2) {
        Left(s"First transaction tree has $rootNodes1 root nodes while second has $rootNodes2")
      } else {
        for {
          rootEvs1 <- t1.rootEventIds.toList.traverse(id => lookupEv(t1.eventsById, id))
          rootEvs2 <- t2.rootEventIds.toList.traverse(id => lookupEv(t2.eventsById, id))
          _ <- rootEvs1.zip(rootEvs2).traverse { case (ev1, ev2) =>
            unify(t1.eventsById, t2.eventsById, ev1, ev2)
          }
        } yield ()
      }
    }

    private def lookupEv(evs: Map[String, TreeEvent], id: String): Either[String, TreeEvent] =
      evs.get(id).toRight(s"Event id not found: $id")

    def unify(
        evs1: Map[String, TreeEvent],
        evs2: Map[String, TreeEvent],
        ev1: TreeEvent,
        ev2: TreeEvent,
    ): Either[String, Unit] =
      (ev1.kind, ev2.kind) match {
        case (TreeEvent.Kind.Empty, TreeEvent.Kind.Empty) => Right(())
        case (TreeEvent.Kind.Created(c1), TreeEvent.Kind.Created(c2)) =>
          unify(c1, c2)
        case (TreeEvent.Kind.Exercised(e1), TreeEvent.Kind.Exercised(e2)) =>
          unify(evs1, evs2, e1, e2)
        case _ => Left(s"Mismatched event types: $ev1, $ev2")
      }

    def unify(
        c1: CreatedEvent,
        c2: CreatedEvent,
    ): Either[String, Unit] =
      for {
        _ <- unifyCid(ContractId(c1.contractId), ContractId(c2.contractId))
        _ <- equal(c1.templateId, c2.templateId)
        _ <- unify(c1.getCreateArguments, c2.getCreateArguments)
      } yield ()

    def unify(r1: v.Record, r2: v.Record): Either[String, Unit] =
      unify(v.Value().withRecord(r1), v.Value().withRecord(r2))

    // Note that we don’t want to test the correctness of the ledger
    // API so we can rely on the values having the same type (we check
    // template types for events) and omit some checks.
    def unify(v1: v.Value, v2: v.Value): Either[String, Unit] =
      (v1.sum, v2.sum) match {
        case (v.Value.Sum.Empty, v.Value.Sum.Empty) => Right(())
        case (v.Value.Sum.Record(r1), v.Value.Sum.Record(r2)) =>
          r1.fields.zip(r2.fields).toList.traverse_ { case (f1, f2) =>
            unify(f1.getValue, f2.getValue)
          }
        case (v.Value.Sum.Variant(var1), v.Value.Sum.Variant(var2)) =>
          for {
            _ <- equal(var1.constructor, var2.constructor)
            _ <- unify(var1.getValue, var2.getValue)
          } yield ()
        case (v.Value.Sum.ContractId(c1), v.Value.Sum.ContractId(c2)) =>
          unifyCid(ContractId(c1), ContractId(c2))
        case (v.Value.Sum.List(l1), v.Value.Sum.List(l2)) =>
          if (l1.elements.length == l2.elements.length) {
            l1.elements.zip(l2.elements).toList.traverse_ { case (e1, e2) =>
              unify(e1, e2)
            }
          } else {
            Left(s"Lists have different number of elements: $l1, $l2")
          }
        case (v.Value.Sum.Int64(i1), v.Value.Sum.Int64(i2)) =>
          equal(i1, i2)
        case (v.Value.Sum.Numeric(n1), v.Value.Sum.Numeric(n2)) =>
          equal(n1, n2)
        case (v.Value.Sum.Text(t1), v.Value.Sum.Text(t2)) =>
          equal(t1, t2)
        case (v.Value.Sum.Timestamp(t1), v.Value.Sum.Timestamp(t2)) =>
          equal(t1, t2)
        case (v.Value.Sum.Party(p1), v.Value.Sum.Party(p2)) =>
          unifyParty(Party(p1), Party(p2))
        case (v.Value.Sum.Bool(b1), v.Value.Sum.Bool(b2)) =>
          equal(b1, b2)
        case (v.Value.Sum.Unit(u1), v.Value.Sum.Unit(u2)) =>
          equal(u1, u2)
        case (v.Value.Sum.Date(d1), v.Value.Sum.Date(d2)) =>
          equal(d1, d2)
        case (v.Value.Sum.Optional(o1), v.Value.Sum.Optional(o2)) =>
          (o1.value, o2.value) match {
            case (None, None) => Right(())
            case (Some(v1), Some(v2)) => unify(v1, v2)
            case _ => Left(s"Optional mismatch: $o1, $o2")
          }
        case (v.Value.Sum.Map(m1), v.Value.Sum.Map(m2)) =>
          if (m1.entries.length == m2.entries.length) {
            m1.entries.zip(m2.entries).toList.traverse_ { case (e1, e2) =>
              for {
                _ <- equal(e1.key, e2.key)
                _ <- unify(e2.getValue, e2.getValue)
              } yield ()
            }
          } else {
            Left(s"TextMaps have different number of elements: $m1, $m2")
          }
        case (v.Value.Sum.Enum(e1), v.Value.Sum.Enum(e2)) =>
          equal(e1, e2)
        case (v.Value.Sum.GenMap(m1), v.Value.Sum.GenMap(m2)) =>
          if (m1.entries.length == m2.entries.length) {
            m1.entries.zip(m2.entries).toList.traverse_ { case (e1, e2) =>
              for {
                _ <- unify(e1.getKey, e2.getKey)
                _ <- unify(e2.getValue, e2.getValue)
              } yield ()
            }
          } else {
            Left(s"Maps have different number of elements: $m1, $m2")
          }
        case _ => Left(s"Values have different type: $v1, $v2")
      }

    def equal[A](v1: A, v2: A): Either[String, Unit] =
      if (v1 == v2) {
        Right(())
      } else {
        Left(s"$v1 /= $v2")
      }

    def unify(
        evs1: Map[String, TreeEvent],
        evs2: Map[String, TreeEvent],
        e1: ExercisedEvent,
        e2: ExercisedEvent,
    ): Either[String, Unit] =
      for {
        _ <- unifyCid(ContractId(e1.contractId), ContractId(e2.contractId))
        _ <- equal(e1.templateId, e2.templateId)
        _ <- equal(e1.choice, e2.choice)
        _ <-
          if (e1.actingParties.length == e2.actingParties.length) {
            e1.actingParties.zip(e2.actingParties).toList.traverse { case (p1, p2) =>
              unifyParty(Party(p1), Party(p2))
            }
          } else {
            Left(s"Different number of acting parties: ${e1.actingParties}, ${e2.actingParties}")
          }
        _ <- unify(e1.getChoiceArgument, e2.getChoiceArgument)
        _ <-
          if (e1.childEventIds.length == e2.childEventIds.length) {
            for {
              childEvs1 <- e1.childEventIds.toList.traverse(lookupEv(evs1, _))
              childEvs2 <- e2.childEventIds.toList.traverse(lookupEv(evs2, _))
              _ <- childEvs1.zip(childEvs2).traverse { case (ev1, ev2) =>
                unify(evs1, evs2, ev1, ev2)
              }
            } yield ()
          } else {
            Left(s"Different number of child events: ${e1.childEventIds}, ${e2.childEventIds}")
          }
      } yield ()

    def unifyParty(p1: Party, p2: Party): Either[String, Unit] = {
      (partiesLR.get(p1), partiesRL.get(p2)) match {
        case (None, None) =>
          partiesLR += p1 -> p2
          partiesRL += p2 -> p1
          Right(())
        case (Some(p2_), Some(p1_)) if p1 == p1_ && p2 == p2_ =>
          Right(())
        case (p2_, p1_) =>
          Left(s"Tried to unify $p1 and $p2 but found existing mapping $p1 -> $p2_, $p2 -> $p1_")
      }
    }

    def unifyCid(c1: ContractId, c2: ContractId): Either[String, Unit] = {
      (contractIdsLR.get(c1), contractIdsRL.get(c2)) match {
        case (None, None) =>
          contractIdsLR += c1 -> c2
          contractIdsRL += c2 -> c1
          Right(())
        case (Some(c2_), Some(c1_)) if c1 == c1_ && c2 == c2_ =>
          Right(())
        case (c2_, c1_) =>
          Left(s"Tried to unify $c1 and $c2 but found existing mapping $c1 -> $c2_, $c2 -> $c1_")
      }
    }
  }
}
