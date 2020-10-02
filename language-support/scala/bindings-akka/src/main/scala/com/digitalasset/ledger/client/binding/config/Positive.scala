// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.config

import pureconfig.ConfigConvert

import scala.util.{Failure, Success, Try}

class Positive[T: Numeric] private (val value: T) {
  override def toString: String = value.toString
}

object Positive {
  def apply[T](num: T)(implicit numeric: Numeric[T]): Try[Positive[T]] = {
    if (numeric.lteq(num, numeric.zero)) {
      Failure(new IllegalArgumentException(s"$num must be positive."))
    } else {
      Success(new Positive(num))
    }
  }

  def unsafe[T](num: T)(implicit numeric: Numeric[T]): Positive[T] = Positive(num).get

  implicit val configConvertL: ConfigConvert[Positive[Long]] = convertPositive(_.toLong)

  implicit val configConvertI: ConfigConvert[Positive[Int]] = convertPositive(_.toInt)

  private def convertPositive[T: Numeric](readStr: String => T) = {

    ConfigConvert.viaStringTry[Positive[T]]({ s =>
      for {
        number <- Try(readStr(s))
        positive <- apply(number)
      } yield positive
    }, _.toString)
  }
}
