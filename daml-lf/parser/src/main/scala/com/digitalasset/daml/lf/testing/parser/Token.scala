// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import com.daml.lf.data

private[parser] sealed trait Token extends Product with Serializable

private[parser] object Token {

  case object `.` extends Token
  case object `:` extends Token
  case object `,` extends Token
  case object `()` extends Token
  case object `(` extends Token
  case object `)` extends Token
  case object `<` extends Token
  case object `>` extends Token
  case object `{` extends Token
  case object `}` extends Token
  case object `[` extends Token
  case object `]` extends Token
  case object `*` extends Token
  case object `->` extends Token
  case object `@` extends Token
  case object `\\` extends Token
  case object `=` extends Token
  case object `<-` extends Token
  case object `/\\` extends Token
  case object `_` extends Token
  case object `|` extends Token
  case object `cons` extends Token
  case object `nil` extends Token
  case object `some` extends Token
  case object `none` extends Token
  case object `forall` extends Token
  case object `let` extends Token
  case object `in` extends Token
  case object `;` extends Token
  case object `with` extends Token
  case object `case` extends Token
  case object `of` extends Token
  case object `sbind` extends Token
  case object `ubind` extends Token
  case object `create` extends Token
  case object `fetch` extends Token
  case object `exercise` extends Token
  case object `exercise_with_actors` extends Token
  case object `fetch_by_key` extends Token
  case object `lookup_by_key` extends Token
  case object `by` extends Token
  case object `to` extends Token
  case object `to_any` extends Token
  case object `from_any` extends Token
  case object `type_rep` extends Token

  final case class Id(s: String) extends Token
  final case class ContractId(s: String) extends Token
  final case class Timestamp(value: data.Time.Timestamp) extends Token
  final case class Date(value: data.Time.Date) extends Token
  final case class Numeric(value: data.Numeric) extends Token
  final case class Number(value: Long) extends Token
  final case class SimpleString(s: String) extends Token
  final case class Text(s: String) extends Token

}
