// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package svalue

import com.daml.lf.data.{Bytes, FrontStack, FrontStackCons, ImmArray, Utf8}
import com.daml.lf.data.ScalazEqual._
import com.daml.lf.language.TypeOrdering
import com.daml.lf.speedy.SError.SErrorCrash
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.ContractId

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object Ordering extends scala.math.Ordering[SValue] {

  private def compareText(text1: String, text2: String): Int =
    Utf8.Ordering.compare(text1, text2)

  private def compareCid(cid1: ContractId, cid2: ContractId): Int =
    (cid1, cid2) match {
      case (ContractId.V0(s1), ContractId.V0(s2)) =>
        s1 compareTo s2
      case (ContractId.V0(_), ContractId.V1(_, _)) =>
        -1
      case (ContractId.V1(_, _), ContractId.V0(_)) =>
        +1
      case (ContractId.V1(hash1, suffix1), ContractId.V1(hash2, suffix2)) =>
        val c1 = crypto.Hash.ordering.compare(hash1, hash2)
        if (c1 != 0)
          c1
        else if (suffix1.isEmpty == suffix2.isEmpty)
          Bytes.ordering.compare(suffix1, suffix2)
        else
          throw SErrorCrash("Conflicting discriminators between a local and global contract id")
    }

  @tailrec
  // Only value of the same type can be compared.
  private[this] def compareValue(stack0: FrontStack[(SValue, SValue)]): Int =
    stack0 match {
      case FrontStack() =>
        0
      case FrontStackCons(tuple, stack) =>
        val (x, toPush) = tuple.match2 {
          case SUnit => {
            case SUnit =>
              0 -> ImmArray.empty
          }
          case SBool(b1) => {
            case SBool(b2) =>
              (b1 compareTo b2) -> ImmArray.empty
          }
          case SInt64(i1) => {
            case SInt64(i2) =>
              (i1 compareTo i2) -> ImmArray.empty
          }
          case STimestamp(ts1) => {
            case STimestamp(ts2) =>
              (ts1.micros compareTo ts2.micros) -> ImmArray.empty
          }
          case SDate(d1) => {
            case SDate(d2) =>
              (d1.days compareTo d2.days) -> ImmArray.empty
          }
          case SNumeric(n1) => {
            case SNumeric(n2) =>
              (n1 compareTo n2) -> ImmArray.empty
          }
          case SText(t1) => {
            case SText(t2) =>
              (compareText(t1, t2)) -> ImmArray.empty
          }
          case SParty(p1) => {
            case SParty(p2) =>
              (compareText(p1, p2)) -> ImmArray.empty
          }
          case SContractId(coid1: ContractId) => {
            case SContractId(coid2: ContractId) =>
              compareCid(coid1, coid2) -> ImmArray.empty
          }
          case STypeRep(t1) => {
            case STypeRep(t2) =>
              // the type-checker ensures t1 and t2 are comparable,
              TypeOrdering.compare(t1, t2) -> ImmArray.empty
          }
          case SEnum(_, _, rank1) => {
            case SEnum(_, _, rank2) =>
              (rank1 compareTo rank2) -> ImmArray.empty
          }
          case SRecord(_, _, args1) => {
            case SRecord(_, _, args2) =>
              0 -> (args1.iterator().asScala zip args2.iterator().asScala).to[ImmArray]
          }
          case SVariant(_, _, rank1, arg1) => {
            case SVariant(_, _, rank2, arg2) =>
              (rank1 compareTo rank2) -> ImmArray((arg1, arg2))
          }
          case SList(FrontStack()) => {
            case SList(l2) =>
              (false compareTo l2.nonEmpty) -> ImmArray.empty
          }
          case SList(FrontStackCons(head1, tail1)) => {
            case SList(FrontStackCons(head2, tail2)) =>
              0 -> ImmArray((head1, head2), (SList(tail1), SList(tail2)))
            case SList(FrontStack()) =>
              1 -> ImmArray.empty
          }
          case SOptional(v1) => {
            case SOptional(v2) =>
              (v1.nonEmpty compareTo v2.nonEmpty) -> (v1.iterator zip v2.iterator).to[ImmArray]
          }
          case map1: STextMap => {
            case map2: STextMap =>
              0 -> ImmArray((toList(map1), toList(map2)))
          }
          case map1: SGenMap => {
            case map2: SGenMap =>
              0 -> ImmArray((toList(map1), toList(map2)))
          }
          case SStruct(_, args1) => {
            case SStruct(_, args2) =>
              0 -> (args1.iterator().asScala zip args2.iterator().asScala).to[ImmArray]
          }
          case SAny(t1, v1) => {
            case SAny(t2, v2) =>
              // the type-checker ensures t1 and t2 are comparable.
              TypeOrdering.compare(t1, t2) -> ImmArray((v1, v2))
          }
          case SPAP(_, _, _) => {
            case SPAP(_, _, _) =>
              throw SErrorCrash("functions are not comparable")
          }
        }(fallback = throw SErrorCrash("try to compare unrelated type"))
        if (x != 0)
          x
        else
          compareValue(toPush ++: stack)
    }

  def compare(v1: SValue, v2: SValue): Int =
    compareValue(FrontStack((v1, v2)))

}
