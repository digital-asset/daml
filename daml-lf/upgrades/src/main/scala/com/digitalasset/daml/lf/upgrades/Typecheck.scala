// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package upgrades

//import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import scala.util.{Try, Success, Failure}

case class Upgrading[A](past: A, present: A) {
  def map[B](f: A => B): Upgrading[B] = Upgrading(f(past), f(present))
}

case class UpgradeError(message: String) extends Throwable

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
      _ <- Try { upgradedModules.values.map(checkModule(_).get).toSeq }
    } yield ()
  }

  def checkModule(module: Upgrading[Ast.Module]): Try[Unit] = {
    for {
      (existingTemplates, _new) <- checkDeleted(
        module.map(_.templates),
        (_: Ast.Template) => UpgradeError(s"MissingTemplate(t)"),
      )
      _ <- Try { existingTemplates.values.map(checkTemplate(_).get).toSeq }
    } yield ()
  }

  def checkTemplate(template: Upgrading[Ast.Template]): Try[Unit] = {
    val _ = template
    Try(())
  }
}

