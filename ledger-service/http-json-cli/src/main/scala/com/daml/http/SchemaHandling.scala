// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
import scalaz.std.option._
import scalaz.syntax.traverse._

private[http] sealed trait SchemaHandling
private[http] object SchemaHandling {
  private[http] case object ForceCreateAndTerminate extends SchemaHandling
  private[http] case object CheckAndTerminateIfWrong extends SchemaHandling
  private[http] case object CreateOrUpdateAndContinue extends SchemaHandling
  private[http] case object ForceCreateAndContinue extends SchemaHandling

  import scalaz.Validation.{success, failure}
  import scalaz.Validation

  private[http] def optionalSchemaHandlingField(
      m: Map[String, String]
  )(k: String): Either[String, Option[SchemaHandling]] = {
    val parse: String => Validation[String, SchemaHandling] = {
      case "ForceCreateAndTerminate" => success(ForceCreateAndTerminate)
      case "CheckAndTerminateIfWrong" => success(CheckAndTerminateIfWrong)
      case "CreateOrUpdateAndContinue" => success(CreateOrUpdateAndContinue)
      case "ForceCreateAndContinue" => success(ForceCreateAndContinue)
      case opt => failure(s"Unrecognized option $opt")
    }
    m.get(k).traverse(input => parse(input).disjunction).toEither
  }
}
