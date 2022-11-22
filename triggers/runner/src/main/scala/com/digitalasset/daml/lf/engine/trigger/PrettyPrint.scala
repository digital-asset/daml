// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.v1.{value => api}
import org.typelevel.paiges.Doc
import org.typelevel.paiges.Doc.{char, fill, intercalate, str, text}

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
}
