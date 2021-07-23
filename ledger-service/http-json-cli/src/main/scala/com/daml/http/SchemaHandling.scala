// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
import scalaz.std.option._
import scalaz.syntax.traverse._

private[http] sealed trait SchemaHandling
private[http] object SchemaHandling {
  private[http] case object CreateSchema extends SchemaHandling
  private[http] case object Start extends SchemaHandling
  private[http] case object CreateSchemaIfNeededAndStart extends SchemaHandling
  private[http] case object CreateSchemaAndStart extends SchemaHandling

  import scalaz.Validation.{success, failure}
  import scalaz.Validation

  private[http] def optionalSchemaHandlingField(
      m: Map[String, String]
  )(k: String): Either[String, Option[SchemaHandling]] = {
    val parse: String => Validation[String, SchemaHandling] = {
      case "CreateSchema" => success(CreateSchema)
      case "Start" => success(Start)
      case "CreateSchemaIfNeededAndStart" => success(CreateSchemaIfNeededAndStart)
      case "CreateSchemaAndStart" => success(CreateSchemaAndStart)
      case opt => failure(s"Unrecognized option $opt")
    }
    m.get(k).traverse(input => parse(input).disjunction).toEither
  }
}
