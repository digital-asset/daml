// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.participant.util

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data.Numeric
import com.digitalasset.daml.lf.data.LawlessTraversals._
import com.digitalasset.daml.lf.transaction.Node.KeyWithMaintainers
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.api.v1.{value => api}
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp

object LfEngineToApi {

  private[this] type LfValue[+Cid] = Lf[Cid]

  def toApiIdentifier(identifier: Identifier) = {
    api.Identifier(
      identifier.packageId,
      identifier.qualifiedName.module.toString(),
      identifier.qualifiedName.name.toString()
    )
  }

  def toTimestamp(instant: Instant): Timestamp = {
    Timestamp.apply(instant.getEpochSecond, instant.getNano)
  }

  def lfVersionedValueToApiRecord(
      verbose: Boolean,
      recordValue: Lf.VersionedValue[Lf.AbsoluteContractId]): Either[String, api.Record] =
    lfValueToApiRecord(verbose, recordValue.value)

  def lfValueToApiRecord(
      verbose: Boolean,
      recordValue: LfValue[Lf.AbsoluteContractId]): Either[String, api.Record] = {
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

  def lfVersionedValueToApiValue(
      verbose: Boolean,
      value: Lf.VersionedValue[Lf.AbsoluteContractId]): Either[String, api.Value] =
    lfValueToApiValue(verbose, value.value)

  def lfValueToApiValue(
      verbose: Boolean,
      value0: LfValue[Lf.AbsoluteContractId]): Either[String, api.Value] =
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
      case Lf.ValueOptional(o) => // TODO DEL-7054 add test coverage
        o.fold[Either[String, api.Value]](
          Right(api.Value(api.Value.Sum.Optional(api.Optional.defaultInstance))))(v =>
          lfValueToApiValue(verbose, v).map(c =>
            api.Value(api.Value.Sum.Optional(api.Optional(Some(c))))))
      case Lf.ValueTextMap(m) =>
        m.toImmArray.reverse
          .foldLeft[Either[String, List[api.Map.Entry]]](Right(List.empty)) {
            case (Right(list), (k, v)) =>
              lfValueToApiValue(verbose, v).map(w => api.Map.Entry(k, Some(w)) :: list)
            case (left, _) => left
          }
          .map(list => api.Value(api.Value.Sum.Map(api.Map(list))))
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
      case Lf.ValueStruct(_) => Left("structs not allowed")
      case Lf.ValueList(vs) =>
        vs.toImmArray.toSeq.traverseEitherStrictly(lfValueToApiValue(verbose, _)) map { xs =>
          api.Value(api.Value.Sum.List(api.List(xs)))
        }
      case Lf.ValueVariant(tycon, variant, v) =>
        lfValueToApiValue(verbose, v) map { x =>
          api.Value(
            api.Value.Sum.Variant(
              api.Variant(
                tycon.filter(_ => verbose).map(toApiIdentifier),
                variant,
                Some(x)
              )))
        }
      case Lf.ValueEnum(tyCon, value) =>
        Right(
          api.Value(
            api.Value.Sum.Enum(
              api.Enum(
                tyCon.filter(_ => verbose).map(toApiIdentifier),
                value
              ))))
      case Lf.ValueRecord(tycon, fields) =>
        fields.toSeq.traverseEitherStrictly { field =>
          lfValueToApiValue(verbose, field._2) map { x =>
            api.RecordField(
              if (verbose)
                field._1.getOrElse("")
              else
                "",
              Some(x)
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
                apiFields
              )))
        }
    }

  def lfContractKeyToApiValue(
      verbose: Boolean,
      lf: KeyWithMaintainers[Lf.VersionedValue[Lf.AbsoluteContractId]]): Either[String, api.Value] =
    lfVersionedValueToApiValue(verbose, lf.key)

  @throws[RuntimeException]
  def assertOrRuntimeEx[A](failureContext: String, ea: Either[String, A]): A =
    ea.fold(e => throw new RuntimeException(s"Unexpected error when $failureContext: $e"), identity)

}
