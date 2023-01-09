// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphql

import com.daml.navigator.model._
import com.daml.navigator.json.ApiCodecVerbose.JsonImplicits._
import com.daml.navigator.json.DamlLfCodec.JsonImplicits._
import sangria.ast
import sangria.schema._
import sangria.validation.ValueCoercionViolation
import spray.json._

/** Custom GraphQL scalar type for raw JSON values.
  *
  * The custom ScalarType is necessary to deliver raw JSON values from the platform to the client
  * through the Sangria GraphQL mechanism. We currently use such raw JSON values for contract arguments
  * and choice parameters. Their internal structure varies enough that they do not lend themselves well
  * to GraphQL queries, and transmitting them in full has no significant effects on performance (<100kB).
  * There is still the minor inconvenience that we include a transfer format in our model but this may
  * change when we introduce a unified approach to generate all transfer data formats and their
  * marshalling/unmarshalling.
  *
  * Adapted from https://gist.github.com/OlegIlyenko/5b96f4b54f656aac226d3c4bc33fd2a6
  */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Any",
  )
)
object JsonType {

  case object JsonCoercionViolation extends ValueCoercionViolation("Not valid JSON")

  // TODO: handle exceptions
  private def coerceJsonInput[T](
      v: sangria.ast.Value
  )(implicit fmt: JsonFormat[T]): Either[sangria.validation.Violation, T] = v match {
    case ast.StringValue(jsonStr, _, _, _, _) =>
      jsonStr.parseJson match {
        case jsValue: JsValue => Right(jsValue.convertTo[T])
        case _ => Left(JsonCoercionViolation)
      }
    case _ =>
      Left(JsonCoercionViolation)
  }

  private def coerceUserJsonInput[T](
      v: Any
  )(implicit fmt: JsonFormat[T]): Either[sangria.validation.Violation, T] = v match {
    case jsValue: JsValue => Right(jsValue.convertTo[T])
    case _ => Left(JsonCoercionViolation)
  }

  private def newScalarType[T](name: String)(implicit fmt: JsonFormat[T]): ScalarType[T] =
    ScalarType(
      name,
      coerceOutput = (value, _) => value.toJson,
      coerceUserInput = coerceUserJsonInput[T],
      coerceInput = coerceJsonInput[T],
    )

  // ------------------------------------------------------------------------------------------------------------------
  // Untyped JSON values???
  // ------------------------------------------------------------------------------------------------------------------
  def apply(name: String, description: Option[String] = None): ScalarType[JsValue] =
    ScalarType[JsValue](
      name,
      description = description,
      coerceOutput = (value, _) => value,
      coerceUserInput = {
        case v: String => Right(JsString(v): JsValue)
        case v: Boolean => Right(JsBoolean(v))
        case v: Int => Right(JsNumber(v))
        case v: Long => Right(JsNumber(v))
        case v: Float => Right(JsNumber(v.toDouble): JsValue)
        case v: Double => Right(JsNumber(v): JsValue)
        case v: BigInt => Right(JsNumber(v))
        case v: BigDecimal => Right(JsNumber(v))
        case v: JsValue => Right(v)
      },
      coerceInput = {
        case ast.StringValue(jsonStr, _, _, _, _) =>
          Right(jsonStr.parseJson)
        case _ =>
          Left(JsonCoercionViolation)
      },
    )

  // ------------------------------------------------------------------------------------------------------------------
  // Ledger API values
  // ------------------------------------------------------------------------------------------------------------------
  val ApiValueType: ScalarType[ApiValue] = newScalarType[ApiValue]("DamlLfValue")
  val ApiRecordType: ScalarType[ApiRecord] = newScalarType[ApiRecord]("DamlLfValueRecord")

  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF types
  // ------------------------------------------------------------------------------------------------------------------
  val DamlLfTypeType: ScalarType[DamlLfType] = newScalarType[DamlLfType]("DamlLfType")
  val DamlLfDataTypeType: ScalarType[DamlLfDataType] =
    newScalarType[DamlLfDataType]("DamlLfDataType")

}
