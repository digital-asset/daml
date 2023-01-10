// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import spray.json.{CompactPrinter, JsArray, JsNumber, JsObject, JsString, JsValue}

/** A limited implementation of JCS https://datatracker.ietf.org/doc/html/rfc8785
  *
  * The limitations of this implementation are that:
  * - only whole numbers are supported
  * - only numbers with an absolute value less than pow(2,52) are supported
  *
  * The primary reason for the above is that this is suitable for our needs and avoids
  * getting into difficult areas regarding the JCS spec
  *
  * For details see JCS.md
  */
object Jcs {

  import scalaz._
  import scalaz.syntax.traverse._
  import std.either._
  import std.vector._

  val MaximumSupportedAbsSize: BigDecimal = BigDecimal(2).pow(52)
  type FailOr[T] = Either[String, T]

  def serialize(json: JsValue): FailOr[String] = json match {
    case JsNumber(value) if !value.isWhole =>
      Left(s"Only whole numbers are supported, not $value")
    case JsNumber(value) if value.abs >= MaximumSupportedAbsSize =>
      Left(
        s"Only numbers with an abs size less than $MaximumSupportedAbsSize are supported, not $value"
      )
    case JsNumber(value) =>
      Right(value.toBigInt.toString())
    case JsArray(elements) =>
      elements.traverse(serialize).map(_.mkString("[", ",", "]"))
    case JsObject(fields) =>
      fields.toVector.sortBy(_._1).traverse(serializePair).map(_.mkString("{", ",", "}"))
    case leaf => Right(compact(leaf))
  }

  private def compact(v: JsValue): String = CompactPrinter(v)

  private def serializePair(kv: (String, JsValue)): FailOr[String] = {
    val (k, v) = kv
    serialize(v).map(vz => s"${compact(JsString(k))}:$vz")
  }

}
