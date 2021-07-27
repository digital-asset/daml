// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import scala.collection.compat._

private[http] sealed trait DbStartupMode
private[http] object DbStartupMode {
  private[http] case object CreateOnly extends DbStartupMode
  private[http] case object StartOnly extends DbStartupMode
  private[http] case object CreateIfNeededAndStart extends DbStartupMode
  private[http] case object CreateAndStart extends DbStartupMode

  private[http] def getConfigValue(startupMode: DbStartupMode) = startupMode match {
    case CreateOnly => "create-only"
    case StartOnly => "start-only"
    case CreateIfNeededAndStart => "create-if-needed-and-start"
    case CreateAndStart => "create-and-start"
  }

  private[http] val allValues: Vector[DbStartupMode] =
    Vector(
      CreateOnly: DbStartupMode,
      StartOnly: DbStartupMode,
      CreateIfNeededAndStart: DbStartupMode,
      CreateAndStart: DbStartupMode,
    )

  private[http] val configValuesMap =
    Map.from(allValues.map(value => (getConfigValue(value), value)))
  private[http] val allConfigValues = configValuesMap.keys.toVector

  import scalaz.Validation.{success, failure}
  import scalaz.Validation
  import scalaz.std.option._
  import scalaz.syntax.traverse._

  private[http] def optionalSchemaHandlingField(
      m: Map[String, String]
  )(k: String): Either[String, Option[DbStartupMode]] = {
    def parse(value: String): Validation[String, DbStartupMode] =
      configValuesMap.get(value) match {
        case Some(res) => success(res)
        case None => failure(s"Unrecognized option $value")
      }
    m.get(k).traverse(input => parse(input).disjunction).toEither
  }
}
