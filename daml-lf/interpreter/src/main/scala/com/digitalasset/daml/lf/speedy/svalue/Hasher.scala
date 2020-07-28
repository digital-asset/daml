// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy.svalue

import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3

private[speedy] object Hasher {

  case class NonHashableSValue(msg: String) extends IllegalArgumentException

  private sealed trait Command

  // mix a block of data with the head of the stack
  private final case class Mix(data: Int) extends Command
  // mix way the n first elements of the stack in a single block
  private final case class Ordered(n: Int) extends Command
  // mix in a symmetric way the n first elements of the stack in a single block
  private final case class Unordered(n: Int) extends Command
  // compute the hash of a value
  private final case class Value(v: SValue) extends Command

  def hash(v: SValue): Int =
    loop(List(Value(v)))

  private def pushOrderedValues(size: Int, values: Iterator[SValue], cmds: List[Command]) =
    (values foldLeft (Ordered(size) :: cmds)) { case (acc, v) => Value(v) :: acc }

  @tailrec
  private def loop(cmds: List[Command], stack: List[Int] = List.empty): Int =
    cmds match {
      case cmd :: cmdsRest =>
        cmd match {
          case Value(v) =>
            v match {
              case _: SPAP =>
                throw NonHashableSValue("function are not hashable")
              case SToken =>
                throw NonHashableSValue("Token are not hashable")
              case STNat(_) =>
                throw NonHashableSValue("STNat value are not hashable")
              case SUnit =>
                loop(cmdsRest, 0 :: stack)
              case SBool(b) =>
                loop(cmdsRest, b.hashCode() :: stack)
              case SInt64(i) =>
                loop(cmdsRest, i.toInt :: stack)
              case SNumeric(n) =>
                loop(cmdsRest, n.hashCode() :: stack)
              case SText(s) =>
                loop(cmdsRest, s.hashCode :: stack)
              case SDate(d) =>
                loop(cmdsRest, d.hashCode() :: stack)
              case STimestamp(t) =>
                loop(cmdsRest, t.hashCode() :: stack)
              case SParty(p) =>
                loop(cmdsRest, p.hashCode :: stack)
              case SContractId(cid) =>
                loop(cmdsRest, cid.hashCode :: stack)
              case STypeRep(t) =>
                loop(cmdsRest, t.hashCode() :: stack)
              case SEnum(_, _, rank) =>
                loop(cmdsRest, rank :: stack)
              case SRecord(_, _, values) =>
                loop(pushOrderedValues(values.size, values.iterator().asScala, cmdsRest), stack)
              case SVariant(_, _, rank, value) =>
                loop(Value(value) :: Mix(rank) :: cmdsRest, stack)
              case SStruct(_, values) =>
                loop(pushOrderedValues(values.size, values.iterator().asScala, cmdsRest), stack)
              case SOptional(opt) =>
                loop(pushOrderedValues(opt.fold(0)(_ => 1), opt.iterator, cmdsRest), stack)
              case SList(values) =>
                loop(pushOrderedValues(values.length, values.iterator, cmdsRest), stack)
              case STextMap(value) =>
                val newCmds = (value foldLeft (Unordered(value.size) :: cmdsRest)) {
                  case (acc, (k, v)) => Value(v) :: Mix(k.hashCode) :: acc
                }
                loop(newCmds, stack)
              case SGenMap(values) =>
                val newCmds = (values foldLeft (Unordered(values.size) :: cmdsRest)) {
                  case (acc, (k, v)) => Value(v) :: Mix(k.hashCode) :: acc
                }
                loop(newCmds, stack)
              case SAny(t, v) =>
                loop(Value(v) :: Mix(t.hashCode()) :: cmdsRest, stack)
            }
          case Mix(h) =>
            val x :: stackRest = stack
            loop(cmdsRest, MurmurHash3.mix(h, x) :: stackRest)
          case Ordered(n) =>
            val (xs, stackRest) = stack.splitAt(n)
            loop(cmdsRest, MurmurHash3.orderedHash(xs) :: stackRest)
          case Unordered(n) =>
            val (xs, stackRest) = stack.splitAt(n)
            loop(cmdsRest, MurmurHash3.unorderedHash(xs) :: stackRest)
        }
      case _ =>
        stack.head
    }

}
