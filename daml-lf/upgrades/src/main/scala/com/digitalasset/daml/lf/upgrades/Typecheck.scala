// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package upgrades

//import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import scala.util.{Try, Success, Failure}
import com.daml.lf.validation.AlphaEquiv
import com.daml.lf.data._

case class Upgrading[A](past: A, present: A) {
  def map[B](f: A => B): Upgrading[B] = Upgrading(f(past), f(present))
  def fold[B](f: (A, A) => B): B = f(past, present)
  def zip[B, C](that: Upgrading[B], f: (A, B) => C) = Upgrading(f(this.past, that.past), f(this.present, that.present))
}

case class UpgradeError(message: String) extends Throwable(message)

sealed abstract class UpgradedRecordOrigin

final case class TemplateBody(template: Ref.DottedName) extends UpgradedRecordOrigin
final case class TemplateChoiceInput(template: Ref.DottedName, choice: Ref.ChoiceName) extends UpgradedRecordOrigin
final case object TopLevel extends UpgradedRecordOrigin

object Typecheck {
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

  def checkPackage(
      present: (Ref.PackageId, Ast.Package),
      past: (Ref.PackageId, Ast.Package),
  ): Try[Unit] = {
    println(s"Upgrading typecheck $past, $present")
    val package_ = Upgrading(past._2, present._2)
    for {
      (upgradedModules, newModules @ _) <-
        checkDeleted(
          package_.map(_.modules),
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

    def dataTypeOriginAndShouldCheck(module: Ast.Module, name: Ref.DottedName): (UpgradedRecordOrigin, Boolean) = {
      module.templates.get(name) match {
        case Some(template) => (TemplateBody(name), true);
        case _ => {
          val choices = for {
            (templateName, template) <- module.templates
            (choiceName, choice) <- template.choices
          } yield {
            val prefix = templateName.segments.init
            val fullName = prefix.slowSnoc(choiceName)
            (Ref.DottedName.unsafeFromNames(fullName), (templateName, choiceName))
          }

          choices.get(name) match {
            case Some((templateName, choiceName)) => (TemplateChoiceInput(templateName, choiceName), true);
            case _ => (TopLevel, true)
          }
        }
      }
    }

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
      _ <- Try {
        existingDatatypes.map({ case (dtName, dt) => {
          val originAndShouldCheck = module.map(dataTypeOriginAndShouldCheck(_, dtName))
          if (originAndShouldCheck.present._1 != originAndShouldCheck.past._1) {
            Failure(UpgradeError("RecordChangedOrigin"))
          } else {
            if (originAndShouldCheck.present._2) {
              checkDatatype(originAndShouldCheck.present._1, dt)
            } else {
              Success(())
            }
          }
        } }).map(_.get)
      }
    } yield ()
  }

  def checkTemplate(template: Upgrading[Ast.Template]): Try[Unit] = {
    for {
      (existingChoices, _newChoices) <- checkDeleted(
        template.map(_.choices),
        (_: Ast.TemplateChoice) => UpgradeError(s"MissingChoice(t)"),
      )

      _ <- tryAll(existingChoices.values, checkChoice(_))
    } yield ()
  }

  def checkChoice(choice: Upgrading[Ast.TemplateChoice]): Try[Unit] = {
    val returnTypes = choice.map(_.returnType)
    val typesMatch = AlphaEquiv.alphaEquiv(returnTypes.past, returnTypes.present)
    if (typesMatch) {
      Success(())
    } else {
      Failure(UpgradeError("ChoiceChangedReturnType"))
    }
  }

  def checkDatatype(origin: UpgradedRecordOrigin, datatype: Upgrading[Ast.DDataType]): Try[Unit] = {
    datatype.map(_.cons) match {
      case Upgrading(past: Ast.DataRecord, present: Ast.DataRecord) => checkFields(origin, Upgrading(past, present))
      case Upgrading(past: Ast.DataVariant, present: Ast.DataVariant) => Try(())
      case Upgrading(past: Ast.DataEnum, present: Ast.DataEnum) => Try(())
      case Upgrading(Ast.DataInterface, Ast.DataInterface) => Try(())
      case _ => Failure(UpgradeError(s"MismatchDataConsVariety"))
    }
  }

  def checkFields(origin: UpgradedRecordOrigin, records: Upgrading[Ast.DataRecord]): Try[Unit] = {
    val fields: Upgrading[Map[Ast.FieldName, Ast.Type]] =
          records.map(rec => Map.from(rec.fields.iterator))
    def fieldTypeUnchanged(typ: Upgrading[Ast.Type]): Boolean =
          AlphaEquiv.alphaEquiv(typ.past, typ.present)
    def fieldTypeOptional(typ: Ast.Type): Boolean =
          typ match {
            case Ast.TApp(Ast.TBuiltin(Ast.BTOptional), _) => false
            case _ => true
          }

    val (_deleted, _existing, _new_) = extractDelExistNew(fields)
    if (_deleted.nonEmpty) {
      Failure(UpgradeError("RecordFieldsMissing"))
    } else if (! _existing.forall({ case (_field, typ) => fieldTypeUnchanged(typ) })) {
      Failure(UpgradeError("RecordFieldsExistingChanged"))
    } else if (_new_.find({ case (field, typ) => !fieldTypeOptional(typ) }).nonEmpty) {
      Failure(UpgradeError("RecordFieldsNewNonOptional"))
    } else {
      Try(())
    }
  }
}

