// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

case class NamePicker(alphabet: String) {
  private[testtool] val canon: String = alphabet.sorted.foldLeft("")((acc, e) => {
    acc.lastOption match {
      case Some(l) if l == e => acc
      case _ => acc + e
    }
  })

  private[testtool] val mx: Char = canon.last
  private[testtool] val mn: Char = canon.head

  private[testtool] def belongs(s: String): Boolean = s.foldLeft(true) {
    case (true, e) if canon.contains(e) => true
    case (_, _) => false
  }

  private[testtool] def allLowest(s: String): Boolean = s.foldLeft(true) {
    case (true, e) if canon.headOption.contains(e) => true
    case (_, _) => false
  }

  private[testtool] def lower(c: Char): Option[Char] = {
    canon.foldLeft(None: Option[Char]) {
      case (_, _) if !canon.contains(c) => None
      case (_, e) if c > e => Some(e)
      case (other, _) => other
    }
  }

  def lower(s: String): Option[String] = s match {
    case s if !belongs(s) => None
    case s if allLowest(s) => s.lastOption.map(_ => s.dropRight(1))
    case s =>
      s.reverse.toList match {
        case last :: init if lower(last).isDefined => Some((init.reverse ++ lower(last)).mkString)
        case last :: Nil => Some(lower(last).toList.mkString)
        case _ :: init => lower(init.reverse.mkString).map(_ + mx)
        case Nil => None
      }
  }

  def lowerConstrained(high: String, low: String, extendOnConflict: Int = 5): Option[String] =
    (high, low) match {
      case (h, l) if !belongs(h) || !belongs(l) => None
      case (h, l) if h <= l => None
      case (h, l) if h.length == l.length + 1 && h.last == mn && h.dropRight(1) == l => None
      case (h, l) => lower(h).map(ld => if (ld == l) ld ++ (mx.toString * extendOnConflict) else ld)
    }
}
