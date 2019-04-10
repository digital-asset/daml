// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.participant.util

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName, SimpleString}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.engine.{
  Command => LfCommand,
  Commands => LfCommands,
  CreateCommand => LfCreateCommand,
  Error => LfError,
  ExerciseCommand => LfExerciseCommand
}
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.api.domain.{
  CreateCommand,
  ExerciseCommand,
  Command => ApiCommand,
  Commands => ApiCommands,
  Identifier => ApiIdentifier,
  RecordField => ApiRecordField,
  Value => ApiValue
}
import com.digitalasset.platform.common.PlatformTypes.asVersionedValueOrThrow
import com.google.protobuf.timestamp.Timestamp
import scalaz.Tag
import scalaz.syntax.tag._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

object ApiToLfEngine {
  sealed trait ApiToLfResult[A] {
    import ApiToLfResult._

    def consume(pcs: PackageId => Option[Package]): Either[LfError, A] = {
      @tailrec
      def go(res: ApiToLfResult[A]): Either[LfError, A] =
        res match {
          case Done(x) => Right(x)
          case Error(err) => Left(err)
          case NeedPackage(pkgId, resume) => go(resume(pcs(pkgId)))
        }
      go(this)
    }

    def consumeAsync(pcs: PackageId => Future[Option[Package]])(
        implicit ec: ExecutionContext): Future[Either[LfError, A]] = {
      def go(res: ApiToLfResult[A]): Future[Either[LfError, A]] = res match {
        case Done(x) => Future.successful(Right(x))
        case Error(err) => Future.successful(Left(err))
        case NeedPackage(pkgId, resume) => pcs(pkgId).flatMap(p => go(resume(p)))
      }
      go(this)
    }

    def flatMap[B](f: A => ApiToLfResult[B]): ApiToLfResult[B] = this match {
      case Done(x) => f(x)
      case Error(err) => Error(err)
      case NeedPackage(pkgId, resume) => NeedPackage(pkgId, mbPkg => resume(mbPkg).flatMap(f))
    }

    def map[B](f: A => B): ApiToLfResult[B] = this match {
      case Done(x) => Done(f(x))
      case Error(err) => Error(err)
      case NeedPackage(pkgId, resume) => NeedPackage(pkgId, mbPkg => resume(mbPkg).map(f))
    }
  }

  object ApiToLfResult {
    final case class Done[A](x: A) extends ApiToLfResult[A]
    final case class Error[A](err: LfError) extends ApiToLfResult[A]
    final case class NeedPackage[A](pkgId: PackageId, resume: Option[Package] => ApiToLfResult[A])
        extends ApiToLfResult[A]
  }

  import ApiToLfResult._

  type LfValue = Lf[AbsoluteContractId]

  def parseDecimal(d: String): Either[String, Decimal.Decimal] = {
    val expectedDecimalFormat = """^[+-]?\d+(\.\d+)?$""".r
    // note that it is important to get the Decimal with Decimal.fromString even if
    // we've already checked the string, because fromString checks that the decimal is within bounds.
    val notValidErr: Either[String, Decimal.Decimal] =
      Left(s"Failed to parse as decimal. ($d). Expected format is [+-]?\\d+(\\.\\d+)?")
    expectedDecimalFormat
      .findFirstIn(d)
      .fold(notValidErr)(
        (_) =>
          // note: using big decimal constructor
          // to have equal precision set as with damle core
          // remove this when removing old engine
          Decimal.fromString(d).map(_ => BigDecimal(d)))
  }

  def toLfIdentifier(
      packages: Map[PackageId, Package],
      apiIdent: ApiIdentifier): ApiToLfResult[(Map[PackageId, Package], Identifier)] = {
    PackageId.fromString(apiIdent.packageId.unwrap) match {
      case Left(err) => ApiToLfResult.Error(LfError(err))
      case Right(pkgId) =>
        packages.get(pkgId) match {
          case None =>
            NeedPackage(pkgId, {
              case None => Error(LfError(s"Could not find package $pkgId"))
              case Some(pkg) => toLfIdentifier(packages + (pkgId -> pkg), apiIdent)
            })
          case Some(pkg) =>
            (
              Ref.ModuleName.fromString(apiIdent.moduleName),
              Ref.DottedName.fromString(apiIdent.entityName)) match {
              case (Right(moduleName), Right(entityName)) =>
                Done((packages, Identifier(pkgId, QualifiedName(moduleName, entityName))))
              case (Left(err), _) => Error(LfError(err))
              case (_, Left(err)) => Error(LfError(err))
            }
        }
    }
  }

  def toOptionLfIdentifier(packages: Map[PackageId, Package], mbApiIdent: Option[ApiIdentifier])
    : ApiToLfResult[(Map[PackageId, Package], Option[Identifier])] = {
    mbApiIdent match {
      case None => Done((packages, None))
      case Some(apiIdent) =>
        toLfIdentifier(packages, apiIdent).map {
          case (newPackages, ident) => (newPackages, Some(ident))
        }
    }
  }

  type Packages = Map[PackageId, Package]

  def apiValueToLfValueWithPackages(
      packages0: Packages,
      v0: ApiValue): ApiToLfResult[(Packages, LfValue)] = {
    def ok(x: LfValue) = Done((packages0, x))
    v0 match {
      case ApiValue.Int64Value(i) => ok(Lf.ValueInt64(i))
      case ApiValue.UnitValue => ok(Lf.ValueUnit)
      case ApiValue.BoolValue(b) => ok(Lf.ValueBool(b))
      case ApiValue.TimeStampValue(t) => ok(Lf.ValueTimestamp(Time.Timestamp.assertFromLong(t)))
      case ApiValue.TextValue(t) => ok(Lf.ValueText(t))
      case ApiValue.PartyValue(p) => ok(Lf.ValueParty(PackageId.assertFromString(p.unwrap)))
      case ApiValue.OptionalValue(o) => // TODO DEL-7054: add test coverage
        o.map(apiValueToLfValueWithPackages(packages0, _))
          .fold[ApiToLfResult[(Packages, LfValue)]](ok(Lf.ValueOptional(None)))(_.map(v =>
            (v._1, Lf.ValueOptional(Some(v._2)))))
      case ApiValue.DecimalValue(d) =>
        parseDecimal(d)
          .fold[ApiToLfResult[(Packages, LfValue)]](
            err => Error(LfError(err)),
            n => ok(Lf.ValueDecimal(n)))
      case ApiValue.DateValue(d) => ok(Lf.ValueDate(Time.Date.assertFromDaysSinceEpoch(d.toInt)))
      case ApiValue.ContractIdValue(s) => ok(Lf.ValueContractId(Lf.AbsoluteContractId(s.unwrap)))
      case ApiValue.ListValue(xs0) =>
        def goResume(
            pkgs: Packages,
            xs: Seq[ApiValue],
            vs: BackStack[LfValue]): ApiToLfResult[(Packages, LfValue)] =
          go(pkgs, xs, vs)
        @tailrec
        def go(
            pkgs: Packages,
            xs: Seq[ApiValue],
            vs: BackStack[LfValue]): ApiToLfResult[(Packages, LfValue)] = {
          if (xs.isEmpty) {
            Done((pkgs, Lf.ValueList(FrontStack(vs.toImmArray))))
          } else {
            apiValueToLfValueWithPackages(pkgs, xs.head) match {
              case Done((newPkgs, v)) => go(newPkgs, xs.tail, vs :+ v)
              case Error(err) => Error(err)
              case np: NeedPackage[(Packages, LfValue)] =>
                np.flatMap { case (newPkgs, v) => goResume(newPkgs, xs.tail, vs :+ v) }
            }
          }
        }
        go(packages0, xs0, BackStack.empty)
      case ApiValue.RecordValue(mbRecordId, fields0) =>
        toOptionLfIdentifier(packages0, mbRecordId).flatMap {
          case (packages1, identifier) =>
            def goResume(
                pkgs: Packages,
                fields: Seq[ApiRecordField],
                fieldsV: BackStack[(Option[String], LfValue)]): ApiToLfResult[(Packages, LfValue)] =
              go(pkgs, fields, fieldsV)
            @tailrec
            def go(
                pkgs: Packages,
                fields: Seq[ApiRecordField],
                fieldsV: BackStack[(Option[String], LfValue)]): ApiToLfResult[(Packages, LfValue)] =
              if (fields.isEmpty) {
                Done((pkgs, Lf.ValueRecord(identifier, fieldsV.toImmArray)))
              } else {
                val field = fields.head
                if (field.value == null)
                  ApiToLfResult.Error(
                    LfError(s"received null field value for label '${field.label.getOrElse("")}'"))
                else
                  apiValueToLfValueWithPackages(pkgs, field.value) match {
                    case Done((newPkgs, fieldV)) =>
                      go(newPkgs, fields.tail, fieldsV :+ ((Tag.unsubst(field.label), fieldV)))
                    case Error(err) => Error(err)
                    case np: NeedPackage[(Packages, LfValue)] =>
                      np.flatMap {
                        case (newPkgs, fieldV) =>
                          goResume(
                            newPkgs,
                            fields.tail,
                            fieldsV :+ ((Tag.unsubst(field.label), fieldV)))
                      }
                  }
              }

            go(packages1, fields0, BackStack.empty)
        }
      case ApiValue.VariantValue(mbVariantId, constructor, arg) =>
        toOptionLfIdentifier(packages0, mbVariantId).flatMap {
          case (packages1, identifier) =>
            apiValueToLfValueWithPackages(packages1, arg).map {
              case (packages2, argV) =>
                (packages2, Lf.ValueVariant(identifier, constructor.unwrap, argV))
            }
        }
      case ApiValue.MapValue(map) =>
        def goResume(
            pkgs: Packages,
            xs: ImmArray[(String, ApiValue)],
            vs: BackStack[(String, LfValue)]
        ): ApiToLfResult[(Packages, LfValue)] =
          go(pkgs, xs, vs)
        @tailrec
        def go(
            pkgs: Packages,
            xs: ImmArray[(String, ApiValue)],
            vs: BackStack[(String, LfValue)]
        ): ApiToLfResult[(Packages, LfValue)] = {
          xs match {
            case ImmArray() =>
              SortedLookupList
                .fromSortedImmArray(vs.toImmArray)
                .fold[ApiToLfResult[(Packages, LfValue)]](
                  err => Error(LfError.apply(s"internal error : $err")),
                  map => Done((pkgs, Lf.ValueMap(map)))
                )
            case ImmArrayCons((key, value), rest) =>
              apiValueToLfValueWithPackages(pkgs, value) match {
                case Done((newPkgs, v)) => go(newPkgs, rest, vs :+ (key -> v))
                case Error(err) => Error(err)
                case np: NeedPackage[(Packages, LfValue)] =>
                  np.flatMap { case (newPkgs, v) => goResume(newPkgs, rest, vs :+ (key -> v)) }
              }
          }
        }
        go(packages0, map.toImmArray, BackStack.empty)
    }
  }

  def apiValueToLfValue(v: ApiValue): ApiToLfResult[LfValue] =
    apiValueToLfValueWithPackages(Map.empty, v).map(_._2)

  def toInstant(ts: Timestamp): Instant =
    java.time.Instant.ofEpochSecond(ts.seconds, ts.nanos.toLong)

  def apiCommandsToLfCommands(cmd: ApiCommands): ApiToLfResult[LfCommands] = {
    val commands = BackStack.empty[LfCommand]

    // keep go tail rec, but allow resuming after getting the pkg
    def goResume(
        packagesCache: Map[PackageId, Package],
        remainingCommands: immutable.Seq[ApiCommand],
        processed: BackStack[LfCommand]): ApiToLfResult[LfCommands] = {
      go(packagesCache, remainingCommands, processed)
    }

    @tailrec
    def go(
        packages0: Map[PackageId, Package],
        remainingCommands0: immutable.Seq[ApiCommand],
        processed: BackStack[LfCommand]): ApiToLfResult[LfCommands] = {
      if (remainingCommands0.isEmpty) {
        ApiToLfResult.Done(
          LfCommands(
            processed.toImmArray.toSeq,
            Time.Timestamp.assertFromInstant(cmd.ledgerEffectiveTime),
            cmd.workflowId.fold("")(_.unwrap)))
      } else {
        val apiCommand = remainingCommands0.head
        val remainingCommands = remainingCommands0.tail

        // convert the old style tpl id to new style
        val oldStyleTplId = apiCommand match {
          case e: ExerciseCommand => e.templateId
          case c: CreateCommand => c.templateId
        }
        toLfIdentifier(packages0, oldStyleTplId) match {
          case Error(err) => Error(err)
          case np: NeedPackage[(Packages, Identifier)] =>
            // just restart to save tailrec
            np.flatMap { case (packages1, _) => goResume(packages1, remainingCommands0, processed) }
          case Done((packages1, tplId)) =>
            def withChoiceArgument(e: ExerciseCommand, arg: LfValue): LfExerciseCommand =
              LfExerciseCommand(
                tplId,
                e.contractId.unwrap,
                e.choice.unwrap,
                SimpleString.assertFromString(cmd.submitter.unwrap),
                asVersionedValueOrThrow(arg)
              )
            def withCreateArgument(c: CreateCommand, arg: LfValue): LfCreateCommand =
              LfCreateCommand(
                tplId,
                asVersionedValueOrThrow(arg),
              )
            apiCommand match {
              case e: ExerciseCommand =>
                apiValueToLfValueWithPackages(packages1, e.choiceArgument) match {
                  case Error(err) => Error(err)
                  case np: NeedPackage[(Packages, LfValue)] =>
                    np.flatMap {
                      case (packages2, choiceArgument) =>
                        goResume(
                          packages2,
                          remainingCommands,
                          processed :+ withChoiceArgument(e, choiceArgument))
                    }
                  case Done((packages2, choiceArgument)) =>
                    go(
                      packages2,
                      remainingCommands,
                      processed :+ withChoiceArgument(e, choiceArgument))
                }
              case c: CreateCommand =>
                apiValueToLfValueWithPackages(packages1, c.record) match {
                  case Error(err) => Error(err)
                  case np: NeedPackage[(Packages, LfValue)] =>
                    np.flatMap {
                      case (packages2, createArgument) =>
                        goResume(
                          packages2,
                          remainingCommands,
                          processed :+ withCreateArgument(c, createArgument))
                    }
                  case Done((packages2, createArgument)) =>
                    go(
                      packages2,
                      remainingCommands,
                      processed :+ withCreateArgument(c, createArgument))
                }
            }
        }
      }
    }

    go(Map.empty, cmd.commands, BackStack.empty)
  }
}
