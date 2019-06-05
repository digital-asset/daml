// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.participant.util

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data.Decimal
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.engine.DeprecatedIdentifier
import com.digitalasset.daml.lf.transaction.Node.KeyWithMaintainers
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.api.v1.commands.{
  Command => ApiCommand,
  Commands => ApiCommands,
  CreateCommand => ApiCreateCommand,
  ExerciseCommand => ApiExerciseCommand,
  CreateAndExerciseCommand => ApiCreateAndExerciseCommand
}
import com.digitalasset.ledger.api.v1.value.{
  Optional,
  RecordField,
  Identifier => ApiIdentifier,
  List => ApiList,
  Map => ApiMap,
  Record => ApiRecord,
  Value => ApiValue,
  Variant => ApiVariant,
  Enum => ApiEnum,
}

import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp

import scala.annotation.tailrec
import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom

object LfEngineToApi {
  private[this] type LfValue[+Cid] = Lf[Cid]

  def toApiIdentifier(identifier: Identifier) = {
    ApiIdentifier(
      identifier.packageId,
      DeprecatedIdentifier.toString(identifier.qualifiedName),
      identifier.qualifiedName.module.toString(),
      identifier.qualifiedName.name.toString()
    )
  }

  def toTimestamp(instant: Instant): Timestamp = {
    Timestamp.apply(instant.getEpochSecond, instant.getNano)
  }

  def lfVersionedValueToApiRecord(
      verbose: Boolean,
      recordValue: Lf.VersionedValue[Lf.AbsoluteContractId]): Either[String, ApiRecord] =
    lfValueToApiRecord(verbose, recordValue.value)

  def lfValueToApiRecord(
      verbose: Boolean,
      recordValue: LfValue[Lf.AbsoluteContractId]): Either[String, ApiRecord] = {
    recordValue match {
      case Lf.ValueRecord(tycon, fields) =>
        val fs = fields.foldLeft[Either[String, Vector[RecordField]]](Right(Vector.empty)) {
          case (Left(e), _) => Left(e)
          case (Right(acc), (mbLabel, value)) =>
            lfValueToApiValue(verbose, value)
              .map(v => RecordField(if (verbose) mbLabel.getOrElse("") else "", Some(v)))
              .map(acc :+ _)
        }
        val mbId = if (verbose) {
          tycon.map(toApiIdentifier)
        } else {
          None
        }

        fs.map(ApiRecord(mbId, _))
      case other =>
        Left(s"Expected value to be record, but got $other")
    }

  }

  def lfVersionedValueToApiValue(
      verbose: Boolean,
      value: Lf.VersionedValue[Lf.AbsoluteContractId]): Either[String, ApiValue] =
    lfValueToApiValue(verbose, value.value)

  def lfValueToApiValue(
      verbose: Boolean,
      value0: LfValue[Lf.AbsoluteContractId]): Either[String, ApiValue] =
    value0 match {
      case Lf.ValueUnit => Right(ApiValue(ApiValue.Sum.Unit(Empty())))
      case Lf.ValueDecimal(d) =>
        Right(ApiValue(ApiValue.Sum.Decimal(Decimal.toString(d))))
      case Lf.ValueContractId(c) => Right(ApiValue(ApiValue.Sum.ContractId(c.coid)))
      case Lf.ValueBool(b) => Right(ApiValue(ApiValue.Sum.Bool(b)))
      case Lf.ValueDate(d) => Right(ApiValue(ApiValue.Sum.Date(d.days)))
      case Lf.ValueTimestamp(t) => Right(ApiValue(ApiValue.Sum.Timestamp(t.micros)))
      case Lf.ValueInt64(i) => Right(ApiValue(ApiValue.Sum.Int64(i)))
      case Lf.ValueParty(p) => Right(ApiValue(ApiValue.Sum.Party(p)))
      case Lf.ValueText(t) => Right(ApiValue(ApiValue.Sum.Text(t)))
      case Lf.ValueOptional(o) => // TODO DEL-7054 add test coverage
        o.fold[Either[String, ApiValue]](
          Right(ApiValue(ApiValue.Sum.Optional(Optional.defaultInstance))))(v =>
          lfValueToApiValue(verbose, v).map(c =>
            ApiValue(ApiValue.Sum.Optional(Optional(Some(c))))))
      case Lf.ValueMap(m) =>
        m.toImmArray.reverse
          .foldLeft[Either[String, List[ApiMap.Entry]]](Right(List.empty[ApiMap.Entry])) {
            case (Right(list), (k, v)) =>
              lfValueToApiValue(verbose, v).map(w => ApiMap.Entry(k, Some(w)) :: list)
            case (left, _) => left
          }
          .map(list => ApiValue(ApiValue.Sum.Map(ApiMap(list))))
      case Lf.ValueTuple(_) => Left("tuples not allowed")
      case Lf.ValueList(vs) =>
        traverseEitherStrictly(vs.toImmArray.toSeq)(lfValueToApiValue(verbose, _)) map { xs =>
          ApiValue(ApiValue.Sum.List(ApiList(xs)))
        }
      case Lf.ValueVariant(tycon, variant, v) =>
        lfValueToApiValue(verbose, v) map { x =>
          ApiValue(
            ApiValue.Sum.Variant(
              ApiVariant(
                tycon.filter(_ => verbose).map(toApiIdentifier),
                variant,
                Some(x)
              )))
        }
      case Lf.ValueEnum(tyCon, value) =>
        Right(
          ApiValue(
            ApiValue.Sum.Enum(
              ApiEnum(
                tyCon.filter(_ => verbose).map(toApiIdentifier),
                value
              ))))
      case Lf.ValueRecord(tycon, fields) =>
        traverseEitherStrictly(fields.toSeq) { field =>
          lfValueToApiValue(verbose, field._2) map { x =>
            RecordField(
              if (verbose)
                field._1.getOrElse("")
              else
                "",
              Some(x)
            )
          }
        } map { apiFields =>
          ApiValue(
            ApiValue.Sum.Record(
              ApiRecord(
                if (verbose)
                  tycon.map(toApiIdentifier)
                else
                  None,
                apiFields
              )))
        }
    }

  def lfCommandToApiCommand(
      submitter: String,
      ledgerId: String,
      workflowId: String,
      applicationId: String,
      ledgerEffectiveTime: Option[Timestamp],
      maximumRecordTime: Option[Timestamp],
      cmds: Commands): ApiCommands = {
    val cmdss = cmds.commands.map {
      case CreateCommand(templateId, argument) =>
        ApiCommand(
          ApiCommand.Command.Create(
            ApiCreateCommand(
              Some(toApiIdentifier(templateId)),
              LfEngineToApi.lfVersionedValueToApiRecord(verbose = true, argument).toOption)))
      case ExerciseCommand(templateId, contractId, choiceId, _, argument) =>
        ApiCommand(
          ApiCommand.Command.Exercise(
            ApiExerciseCommand(
              Some(toApiIdentifier(templateId)),
              contractId,
              choiceId,
              LfEngineToApi.lfValueToApiValue(verbose = true, argument.value).toOption)))
      case CreateAndExerciseCommand(templateId, createArgument, choiceId, choiceArgument, _) =>
        ApiCommand(
          ApiCommand.Command.CreateAndExercise(ApiCreateAndExerciseCommand(
            Some(toApiIdentifier(templateId)),
            LfEngineToApi.lfVersionedValueToApiRecord(verbose = true, createArgument).toOption,
            choiceId,
            LfEngineToApi.lfVersionedValueToApiValue(verbose = true, choiceArgument).toOption
          )))
    }

    ApiCommands(
      ledgerId,
      workflowId,
      applicationId,
      cmds.commandsReference,
      submitter,
      ledgerEffectiveTime,
      maximumRecordTime,
      cmdss.toSeq)
  }

  def lfContractKeyToApiValue(
      verbose: Boolean,
      lf: KeyWithMaintainers[Lf.VersionedValue[Lf.AbsoluteContractId]]): Either[String, ApiValue] =
    lfVersionedValueToApiValue(verbose, lf.key)

  /** This traversal fails the identity law so is unsuitable for [[scalaz.Traverse]].
    * It is, nevertheless, what is meant sometimes.
    */
  private[this] def traverseEitherStrictly[A, B, C, This, That](seq: IterableLike[A, This])(
      f: A => Either[B, C])(implicit cbf: CanBuildFrom[This, C, That]): Either[B, That] = {
    val that = cbf()
    that.sizeHint(seq)
    val i = seq.iterator
    @tailrec def lp(): Either[B, That] =
      if (i.hasNext) f(i.next) match {
        case Left(b) => Left(b)
        case Right(c) =>
          that += c
          lp()
      } else Right(that.result)
    lp()
  }
}
