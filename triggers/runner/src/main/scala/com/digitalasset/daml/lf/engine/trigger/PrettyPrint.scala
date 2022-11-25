// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.v1.{value => api}
import com.daml.lf.speedy.{Pretty, SValue}
import org.typelevel.paiges.Doc
import org.typelevel.paiges.Doc.{char, fill, intercalate, str, text}

import scala.jdk.CollectionConverters._

object PrettyPrint {

  def prettyApiIdentifier(id: api.Identifier): Doc =
    text(id.moduleName) + char(':') + text(id.entityName) + char('@') + text(id.packageId)

  def prettyApiValue(verbose: Boolean)(v: api.Value): Doc =
    v.sum match {
      case api.Value.Sum.Empty => Doc.empty

      case api.Value.Sum.Int64(i) => str(i)

      case api.Value.Sum.Numeric(d) => text(d)

      case api.Value.Sum.Record(api.Record(mbId, fs)) =>
        (mbId match {
          case Some(id) if verbose => prettyApiIdentifier(id)
          case _ => Doc.empty
        }) +
          char('{') &
          fill(
            text(", "),
            fs.toList.map {
              case api.RecordField(k, Some(v)) =>
                text(k) & char('=') & prettyApiValue(verbose = true)(v)
              case _ => Doc.empty
            },
          ) &
          char('}')

      case api.Value.Sum.Variant(api.Variant(mbId, variant, value)) =>
        (mbId match {
          case Some(id) if verbose => prettyApiIdentifier(id) + char(':')
          case _ => Doc.empty
        }) +
          text(variant) + char('(') + value.fold(Doc.empty)(v =>
            prettyApiValue(verbose = true)(v)
          ) + char(')')

      case api.Value.Sum.Enum(api.Enum(mbId, constructor)) =>
        (mbId match {
          case Some(id) if verbose => prettyApiIdentifier(id) + char(':')
          case _ => Doc.empty
        }) + text(constructor)

      case api.Value.Sum.Text(t) => char('"') + text(t) + char('"')

      case api.Value.Sum.ContractId(acoid) => text(acoid)

      case api.Value.Sum.Unit(_) => text("<unit>")

      case api.Value.Sum.Bool(b) => str(b)

      case api.Value.Sum.List(api.List(lst)) =>
        char('[') + intercalate(text(", "), lst.map(prettyApiValue(verbose = true)(_))) + char(']')

      case api.Value.Sum.Timestamp(t) => str(t)

      case api.Value.Sum.Date(days) => str(days)

      case api.Value.Sum.Party(p) => char('\'') + str(p) + char('\'')

      case api.Value.Sum.Optional(api.Optional(Some(v1))) =>
        text("Option(") + prettyApiValue(verbose)(v1) + char(')')

      case api.Value.Sum.Optional(api.Optional(None)) => text("None")

      case api.Value.Sum.Map(api.Map(entries)) =>
        val list = entries.map {
          case api.Map.Entry(k, Some(v)) =>
            text(k) + text(" -> ") + prettyApiValue(verbose)(v)
          case _ => Doc.empty
        }
        text("TextMap(") + intercalate(text(", "), list) + text(")")

      case api.Value.Sum.GenMap(api.GenMap(entries)) =>
        val list = entries.map {
          case api.GenMap.Entry(Some(k), Some(v)) =>
            prettyApiValue(verbose)(k) + text(" -> ") + prettyApiValue(verbose)(v)
          case _ => Doc.empty
        }
        text("GenMap(") + intercalate(text(", "), list) + text(")")
    }

  def prettySValue(v: SValue): Doc = v match {
    case SValue.SPAP(_, _, _) =>
      text("...")

    case SValue.SRecord(id, fields, values) =>
      Pretty.prettyIdentifier(id) + char('{') & fill(
        text(", "),
        fields.toSeq.zip(values.asScala).map { case (k, v) =>
          text(k) & char('=') & prettySValue(v)
        },
      ) & char('}')

    case SValue.SStruct(fieldNames, values) =>
      char('<') + fill(
        text(", "),
        fieldNames.names.zip(values.asScala).toSeq.map { case (k, v) =>
          text(k) + char('=') + prettySValue(v)
        },
      ) + char('>')

    case SValue.SVariant(id, variant, _, value) =>
      Pretty.prettyIdentifier(id) + char(':') + text(variant) + char('(') + prettySValue(
        value
      ) + char(')')

    case SValue.SEnum(id, constructor, _) =>
      Pretty.prettyIdentifier(id) + char(':') + text(constructor)

    case SValue.SOptional(Some(value)) =>
      text("Option(") + prettySValue(value) + char(')')

    case SValue.SOptional(None) =>
      text("None")

    case SValue.SList(list) =>
      char('[') + intercalate(text(", "), list.map(prettySValue).toImmArray.toSeq) + char(']')

    case SValue.SMap(isTextMap, entries) =>
      val list = entries.map { case (k, v) =>
        prettySValue(k) + text(" -> ") + prettySValue(v)
      }
      text(if (isTextMap) "TextMap(" else "GenMap(") + intercalate(text(", "), list) + text(")")

    case SValue.SAny(ty, value) =>
      text("to_any") + char('@') + text(ty.pretty) + prettySValue(value)

    case SValue.STNat(n) =>
      str(n)

    case SValue.SInt64(value) =>
      str(value)

    case SValue.SNumeric(value) =>
      str(value)

    case SValue.SBigNumeric(value) =>
      str(value)

    case SValue.SText(value) =>
      text(s"$value")

    case SValue.STimestamp(value) =>
      str(value)

    case SValue.SParty(value) =>
      char('\'') + str(value) + char('\'')

    case SValue.SBool(value) =>
      str(value)

    case SValue.SUnit =>
      text("<unit>")

    case SValue.SDate(value) =>
      str(value)

    case SValue.SContractId(value) =>
      text(value.coid)

    case SValue.STypeRep(ty) =>
      text(ty.pretty)

    case SValue.SToken =>
      text("Token")
  }
}
