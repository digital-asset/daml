// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package upgrades

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import scala.util.{Try, Success, Failure}
import com.daml.lf.validation.AlphaEquiv

case class Upgrading[A](past: A, present: A) {
  def map[B](f: A => B): Upgrading[B] = Upgrading(f(past), f(present))
  def fold[B](f: (A, A) => B): B = f(past, present)
  def zip[B, C](that: Upgrading[B], f: (A, B) => C) =
    Upgrading(f(this.past, that.past), f(this.present, that.present))
}

case class UpgradeError(message: String) extends Throwable(message)

sealed abstract class UpgradedRecordOrigin

final case class TemplateBody(template: Ref.DottedName) extends UpgradedRecordOrigin
final case class TemplateChoiceInput(template: Ref.DottedName, choice: Ref.ChoiceName)
    extends UpgradedRecordOrigin
final case object TopLevel extends UpgradedRecordOrigin

object Typecheck {
  def typecheckUpgrades(
      present: (Ref.PackageId, Ast.Package),
      mbPast: Option[(Ref.PackageId, Ast.Package)],
  ): Try[Unit] = {
    mbPast match {
      case None => Failure(UpgradeError("CouldNotResolveUpgradedPackageId"));
      case Some(past) => {
        val tc = this(Upgrading(past, present))
        tc.check()
      }
    }
  }
}

case class Typecheck(packagesAndIds: Upgrading[(Ref.PackageId, Ast.Package)]) {
  lazy val packageId: Upgrading[Ref.PackageId] = packagesAndIds.map(_._1)
  lazy val _package: Upgrading[Ast.Package] = packagesAndIds.map(_._2)

  def extractDelExistNew[K, V](
      arg: Upgrading[Map[K, V]]
  ): (Map[K, V], Map[K, Upgrading[V]], Map[K, V]) =
    (
      arg.past -- arg.present.keySet,
      arg.past.keySet
        .intersect(arg.present.keySet)
        .map(k => k -> Upgrading(arg.past(k), arg.present(k)))
        .toMap,
      arg.present -- arg.past.keySet,
    )

  def checkDeleted[K, V, T <: Throwable](
      arg: Upgrading[Map[K, V]],
      handler: V => T,
  ): Try[(Map[K, Upgrading[V]], Map[K, V])] = {
    val (deletedV, existingV, newV) = extractDelExistNew(arg)
    deletedV.headOption match {
      case Some((k @ _, v)) => Failure(handler(v))
      case _ => Success((existingV, newV))
    }
  }

  def tryAll[A, B](t: Iterable[A], f: A => Try[B]): Try[Seq[B]] =
    Try { t.map(f(_).get).toSeq }

  def check(): Try[Unit] = {
    for {
      (upgradedModules, newModules @ _) <-
        checkDeleted(
          _package.map(_.modules),
          (m: Ast.Module) => UpgradeError(s"MissingModule(${m.name})"),
        )
      _ <- tryAll(upgradedModules.values, checkModule(_))
    } yield ()
  }

  def checkModule(module: Upgrading[Ast.Module]): Try[Unit] = {
    def datatypes(module: Ast.Module): Map[Ref.DottedName, Ast.DDataType] =
      module.definitions.flatMap(_ match {
        case (k, v: Ast.DDataType) => Some((k, v));
        case _ => None;
      })

    for {
      (existingTemplates, _new) <- checkDeleted(
        module.map(_.templates),
        (_: Ast.Template) => UpgradeError(s"MissingTemplate(t)"),
      )
      _ <- tryAll(existingTemplates.values, checkTemplate(_))

      (existingDatatypes, _new) <- checkDeleted(
        module.map(datatypes(_)),
        (_: Ast.DDataType) => UpgradeError(s"MissingDataCon(t)"),
      )
      _ <- Try { existingDatatypes.map({ case (name, dt) => checkDatatype(module, name, dt).get }) }
    } yield ()
  }

  def checkTemplate(template: Upgrading[Ast.Template]): Try[Unit] = {
    for {
      (existingChoices, _newChoices) <- checkDeleted(
        template.map(_.choices),
        (_: Ast.TemplateChoice) => UpgradeError(s"MissingChoice(t)"),
      )

      _ <- tryAll(existingChoices.values, checkChoice(_))
      _ <- checkKey(template.map(_.key))
    } yield ()
  }

  def checkType(typ: Upgrading[Ast.Type]): Boolean = {
    AlphaEquiv.alphaEquiv(stripPackageIds(typ.past), stripPackageIds(typ.present))
  }

  // TODO: Consider whether we should strip package ids from all packages in the
  // upgrade set, not just within the pair
  def stripPackageIds(typ: Ast.Type): Ast.Type = {
    def stripIdentifier(id: Ref.Identifier): Ref.Identifier =
      if (id.packageId == packageId.present) {
        Ref.Identifier(packageId.past, id.qualifiedName)
      } else {
        id
      }

    typ match {
      case Ast.TNat(n) => Ast.TNat(n)
      case Ast.TSynApp(n, args) => Ast.TSynApp(stripIdentifier(n), args.map(stripPackageIds(_)))
      case Ast.TVar(n) => Ast.TVar(n)
      case Ast.TTyCon(con) => Ast.TTyCon(stripIdentifier(con))
      case Ast.TBuiltin(bt) => Ast.TBuiltin(bt)
      case Ast.TApp(fun, arg) => Ast.TApp(stripPackageIds(fun), stripPackageIds(arg))
      case Ast.TForall(v, body) => Ast.TForall(v, stripPackageIds(body))
      case Ast.TStruct(fields) => Ast.TStruct(fields.mapValues(stripPackageIds(_)))
    }
  }

  def checkKey(key: Upgrading[Option[Ast.TemplateKey]]): Try[Unit] = {
    key match {
      case Upgrading(None, None) => Success(());
      case Upgrading(Some(pastKey), Some(presentKey)) => {
        if (!checkType(Upgrading(pastKey.typ, presentKey.typ)))
          Failure(UpgradeError(s"TemplateChangedKeyType"))
        else
          Success(())
      }
      case Upgrading(Some(pastKey @ _), None) =>
        Failure(UpgradeError(s"TemplateRemovedKey"))
      case Upgrading(None, Some(presentKey @ _)) =>
        Success(
          ()
        ) // TODO: Should emit a warning, but we don't currently have a framework for warnings
    }
  }

  def checkChoice(choice: Upgrading[Ast.TemplateChoice]): Try[Unit] = {
    if (checkType(choice.map(_.returnType))) {
      Success(())
    } else {
      Failure(UpgradeError("ChoiceChangedReturnType"))
    }
  }

  def dataTypeOrigin(
      module: Ast.Module,
      name: Ref.DottedName,
  ): UpgradedRecordOrigin = {
    module.templates.get(name) match {
      case Some(template @ _) => TemplateBody(name);
      case None => {
        val choices = for {
          (templateName, template) <- module.templates
          (choiceName, choice) <- template.choices
        } yield {
          val prefix = templateName.segments.init
          val fullName = prefix.slowSnoc(choiceName)
          (Ref.DottedName.unsafeFromNames(fullName), (templateName, choiceName))
        }

        choices.get(name) match {
          case Some((templateName, choiceName)) =>
            TemplateChoiceInput(templateName, choiceName);
          case _ => TopLevel;
        }
      }
    }
  }

  def checkDatatype(
      module: Upgrading[Ast.Module],
      name: Ref.DottedName,
      datatype: Upgrading[Ast.DDataType],
  ): Try[Unit] = {
    val origin = module.map(dataTypeOrigin(_, name))
    if (origin.present != origin.past) {
      Failure(UpgradeError("RecordChangedOrigin"))
    } else {
      datatype.map(_.cons) match {
        case Upgrading(past: Ast.DataRecord, present: Ast.DataRecord) =>
          checkFields(Upgrading(past, present))
        case Upgrading(_: Ast.DataVariant, _: Ast.DataVariant) => Try(())
        case Upgrading(_: Ast.DataEnum, _: Ast.DataEnum) => Try(())
        case Upgrading(Ast.DataInterface, Ast.DataInterface) => Try(())
        case _ => Failure(UpgradeError(s"MismatchDataConsVariety"))
      }
    }
  }

  def checkFields(records: Upgrading[Ast.DataRecord]): Try[Unit] = {
    val fields: Upgrading[Map[Ast.FieldName, Ast.Type]] =
      records.map(rec => Map.from(rec.fields.iterator))
    def fieldTypeOptional(typ: Ast.Type): Boolean =
      typ match {
        case Ast.TApp(Ast.TBuiltin(Ast.BTOptional), _) => true
        case _ => false
      }

    val (_deleted, _existing, _new_) = extractDelExistNew(fields)
    if (_deleted.nonEmpty) {
      Failure(UpgradeError("RecordFieldsMissing"))
    } else if (!_existing.forall({ case (field @ _, typ) => checkType(typ) })) {
      Failure(UpgradeError("RecordFieldsExistingChanged"))
    } else if (_new_.find({ case (field @ _, typ) => !fieldTypeOptional(typ) }).nonEmpty) {
      Failure(UpgradeError("RecordFieldsNewNonOptional"))
    } else {
      Success(())
    }
  }
}