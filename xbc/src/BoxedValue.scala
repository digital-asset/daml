// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc

sealed abstract class BoxedValue

object BoxedValue {

  final case class Number(x: Long) extends BoxedValue
  final case class String(s: String) extends BoxedValue

  def mul(v1: BoxedValue, v2: BoxedValue): BoxedValue = {
    (v1, v2) match {
      case (Number(x1), Number(x2)) => Number(x1 * x2)
      case _ => sys.error("BoxedValue.mul")
    }
  }

  def add(v1: BoxedValue, v2: BoxedValue): BoxedValue = {
    (v1, v2) match {
      case (Number(x1), Number(x2)) => Number(x1 + x2)
      case _ => sys.error("BoxedValue.add")
    }
  }

  def sub(v1: BoxedValue, v2: BoxedValue): BoxedValue = {
    (v1, v2) match {
      case (Number(x1), Number(x2)) => Number(x1 - x2)
      case _ => sys.error("BoxedValue.sub")
    }
  }

  def cmp(v1: BoxedValue, v2: BoxedValue): BoxedValue = {
    (v1, v2) match {
      case (Number(x1), Number(x2)) => Number(if (x1 > x2) 1 else if (x1 == x2) 0 else -1)
      case _ => sys.error("BoxedValue.cmp")
    }
  }

  def isNeg(v: BoxedValue): Boolean = {
    v match {
      case Number(x) => x < 0
      case _ => sys.error("BoxedValue.isNeg")
    }
  }

  def getNumber(v: BoxedValue): Option[Long] = {
    v match {
      case Number(x) => Some(x)
      case _ => None
    }
  }

}
