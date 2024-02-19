// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import com.daml.ledger.api.v2.value as api
import com.daml.lf.data.{Numeric, Ref}
import com.daml.lf.value.Value as Lf
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

import java.time.Instant

/** Translates [[com.daml.lf.value.Value]] values to Ledger API values.
  *
  * All conversion functions are pure and total.
  *
  * Most conversion functions have a verbose flag:
  * - If verbose mode is disabled, then all resulting Api values have missing type identifiers and record field names.
  * - If verbose mode is enabled, then type identifiers and record field names are copied from the input Daml-LF values.
  *   The caller is responsible for filling in missing type information using [[com.daml.lf.engine.ValueEnricher]],
  *   which may involve loading Daml-LF packages.
  */
object LfEngineToApi {

  private[this] type LfValue = Lf

  def toApiIdentifier(identifier: Ref.Identifier): api.Identifier = {
    api.Identifier(
      identifier.packageId,
      identifier.qualifiedName.module.toString(),
      identifier.qualifiedName.name.toString(),
    )
  }

  def toApiIdentifier(typConRef: Ref.TypeConRef): api.Identifier =
    toApiIdentifier(typConRef.assertToTypeConName)

  def toTimestamp(instant: Instant): Timestamp = {
    Timestamp.apply(instant.getEpochSecond, instant.getNano)
  }

  def lfValueToApiRecord(
      verbose: Boolean,
      recordValue: LfValue,
  ): Either[String, api.Record] = {
    recordValue match {
      case Lf.ValueRecord(tycon, fields) =>
        val fs = fields.foldLeft[Either[String, Vector[api.RecordField]]](Right(Vector.empty)) {
          case (Left(e), _) => Left(e)
          case (Right(acc), (mbLabel, value)) =>
            lfValueToApiValue(verbose, value)
              .map(v => api.RecordField(if (verbose) mbLabel.getOrElse("") else "", Some(v)))
              .map(acc :+ _)
        }
        val mbId = if (verbose) {
          tycon.map(toApiIdentifier)
        } else {
          None
        }

        fs.map(api.Record(mbId, _))
      case other =>
        Left(s"Expected value to be record, but got $other")
    }

  }
  def lfValueToApiValue(
      verbose: Boolean,
      lf: Option[LfValue],
  ): Either[String, Option[api.Value]] =
    lf.fold[Either[String, Option[api.Value]]](Right(None))(
      lfValueToApiValue(verbose, _).map(Some(_))
    )

  def lfValueToApiValue(
      verbose: Boolean,
      value0: LfValue,
  ): Either[String, api.Value] =
    value0 match {
      case Lf.ValueUnit => Right(api.Value(api.Value.Sum.Unit(Empty())))
      case Lf.ValueNumeric(d) =>
        Right(api.Value(api.Value.Sum.Numeric(Numeric.toString(d))))
      case Lf.ValueContractId(c) => Right(api.Value(api.Value.Sum.ContractId(c.coid)))
      case Lf.ValueBool(b) => Right(api.Value(api.Value.Sum.Bool(b)))
      case Lf.ValueDate(d) => Right(api.Value(api.Value.Sum.Date(d.days)))
      case Lf.ValueTimestamp(t) => Right(api.Value(api.Value.Sum.Timestamp(t.micros)))
      case Lf.ValueInt64(i) => Right(api.Value(api.Value.Sum.Int64(i)))
      case Lf.ValueParty(p) => Right(api.Value(api.Value.Sum.Party(p)))
      case Lf.ValueText(t) => Right(api.Value(api.Value.Sum.Text(t)))
      case Lf.ValueOptional(o) => // TODO(i12289) DEL-7054 add test coverage
        o.fold[Either[String, api.Value]](
          Right(api.Value(api.Value.Sum.Optional(api.Optional.defaultInstance)))
        )(v =>
          lfValueToApiValue(verbose, v).map(c =>
            api.Value(api.Value.Sum.Optional(api.Optional(Some(c))))
          )
        )
      case Lf.ValueTextMap(m) =>
        m.toImmArray.reverse
          .foldLeft[Either[String, List[api.TextMap.Entry]]](Right(List.empty)) {
            case (Right(list), (k, v)) =>
              lfValueToApiValue(verbose, v).map(w => api.TextMap.Entry(k, Some(w)) :: list)
            case (left, _) => left
          }
          .map(list => api.Value(api.Value.Sum.TextMap(api.TextMap(list))))
      case Lf.ValueGenMap(entries) =>
        entries.reverseIterator
          .foldLeft[Either[String, List[api.GenMap.Entry]]](Right(List.empty)) {
            case (acc, (k, v)) =>
              for {
                tail <- acc
                key <- lfValueToApiValue(verbose, k)
                value <- lfValueToApiValue(verbose, v)
              } yield api.GenMap.Entry(Some(key), Some(value)) :: tail
          }
          .map(list => api.Value(api.Value.Sum.GenMap(api.GenMap(list))))
      case Lf.ValueList(vs) =>
        vs.toImmArray.toList.traverseU(lfValueToApiValue(verbose, _)) map { xs =>
          api.Value(api.Value.Sum.List(api.List(xs)))
        }
      case Lf.ValueVariant(tycon, variant, v) =>
        lfValueToApiValue(verbose, v) map { x =>
          api.Value(
            api.Value.Sum.Variant(
              api.Variant(
                tycon.filter(_ => verbose).map(toApiIdentifier),
                variant,
                Some(x),
              )
            )
          )
        }
      case Lf.ValueEnum(tyCon, value) =>
        Right(
          api.Value(
            api.Value.Sum.Enum(
              api.Enum(
                tyCon.filter(_ => verbose).map(toApiIdentifier),
                value,
              )
            )
          )
        )
      case Lf.ValueRecord(tycon, fields) =>
        fields.toList.traverseU { field =>
          lfValueToApiValue(verbose, field._2) map { x =>
            api.RecordField(
              if (verbose)
                field._1.getOrElse("")
              else
                "",
              Some(x),
            )
          }
        } map { apiFields =>
          api.Value(
            api.Value.Sum.Record(
              api.Record(
                if (verbose)
                  tycon.map(toApiIdentifier)
                else
                  None,
                apiFields,
              )
            )
          )
        }
    }

  @throws[RuntimeException]
  def assertOrRuntimeEx[A](failureContext: String, ea: Either[String, A]): A =
    ea.fold(e => throw new RuntimeException(s"Unexpected error when $failureContext: $e"), identity)

}
