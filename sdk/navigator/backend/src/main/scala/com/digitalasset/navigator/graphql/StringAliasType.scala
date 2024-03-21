// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphql

import sangria.ast.StringValue
import sangria.schema.ScalarType
import sangria.validation.StringCoercionViolation

import scalaz.{@@, Tag}

/** GraphQL alias type for string value. */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object StringAliasType {
  def apply(name: String, description: Option[String] = None): ScalarType[String] =
    ScalarType(
      name,
      description = description,
      coerceOutput = sangria.schema.valueOutput,
      coerceUserInput = {
        case s: String => Right(s)
        case _ => Left(StringCoercionViolation)
      },
      coerceInput = {
        case StringValue(s, _, _, _, _) => Right(s)
        case _ => Left(StringCoercionViolation)
      },
    )

  def tagged[T](name: String, description: Option[String] = None): ScalarType[String @@ T] =
    ScalarType(
      name,
      description = description,
      coerceOutput = (value, _) => Tag.unwrap(value),
      coerceUserInput = {
        case s: String => Right(Tag.of[T](s))
        case _ => Left(StringCoercionViolation)
      },
      coerceInput = {
        case StringValue(s, _, _, _, _) => Right(Tag.of[T](s))
        case _ => Left(StringCoercionViolation)
      },
    )
}
