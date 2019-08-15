// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.transaction.VersionTimeline
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data.{ImmArray, ImmArrayCons}
import com.digitalasset.daml.lf.language.Ast.Package

import scala.annotation.tailrec

/** After #1866, DAML-LF execution is parametrized by whether we should check
  * if the "transaction submitter" is in the key maintainers for key lookup
  * and fetch.
  *
  * For scenarios we just use the version of the module where the scenario
  * definition comes from. For Ledger API commands, we use the latest version
  * amongst the versions of the modules from where the templates of the commands
  * come from. This file implements the latter scenario.
  *
  * We only return [[ResultError]], [[ResultDone]], and [[ResultNeedPackage]].
  */
object ShouldCheckSubmitterInMaintainers {

  private def templateShouldCheckSubmitterInMaintainers(
      compiledPackages: Option[ConcurrentCompiledPackages],
      templateId: Identifier): Result[Boolean] = {
    def withPkg(pkg: Package): Result[Boolean] =
      pkg.modules.get(templateId.qualifiedName.module) match {
        case None => ResultError(Error(s"Could not find module ${templateId.qualifiedName.module}"))
        case Some(module) =>
          ResultDone(VersionTimeline.checkSubmitterInMaintainers(module.languageVersion))
      }

    compiledPackages match {
      case None => Result.needPackage(templateId.packageId, withPkg)
      case Some(pkgs) => Result.needPackage(pkgs, templateId.packageId, withPkg)
    }
  }

  private def apply(
      compiledPackages: Option[ConcurrentCompiledPackages],
      templates0: ImmArray[Identifier]): Result[Boolean] = {
    // not using [[Result#sequence]] on purpose, see
    // <https://github.com/digital-asset/daml/blob/995ee82fd0655231d7034d0a66c9fe2c6a419536/daml-lf/engine/src/main/scala/com/digitalasset/daml/lf/engine/CommandPreprocessor.scala#L463>
    @tailrec
    def go(
        checkSubmitterInMaintainers: Boolean,
        templates: ImmArray[Identifier]): Result[Boolean] = {
      if (checkSubmitterInMaintainers) {
        ResultDone(true)
      } else {
        templates match {
          case ImmArray() => ResultDone(checkSubmitterInMaintainers)
          case ImmArrayCons(template, rest) =>
            templateShouldCheckSubmitterInMaintainers(compiledPackages, template) match {
              case ResultError(err) => ResultError(err)
              case ResultDone(b) => go(checkSubmitterInMaintainers || b, rest)
              case ResultNeedPackage(pkgId, resume) =>
                ResultNeedPackage(pkgId, { pkg =>
                  resume(pkg).flatMap { b =>
                    goResume(checkSubmitterInMaintainers || b, rest)
                  }
                })
              case result => sys.error(s"Unexpected result: $result")
            }
        }
      }
    }
    def goResume(
        checkSubmitterInMaintainers: Boolean,
        templates: ImmArray[Identifier]): Result[Boolean] =
      go(checkSubmitterInMaintainers, templates)
    // for no commands the version is irrelevant -- we just return
    // the earliest one.
    go(false, templates0)
  }

  def apply(commands: Commands): Result[Boolean] =
    apply(None, commands.commands.map(_.templateId))

  def apply(compiledPackages: ConcurrentCompiledPackages, commands: Commands): Result[Boolean] =
    apply(Some(compiledPackages), commands.commands.map(_.templateId))

  def apply(templates: ImmArray[Identifier]): Result[Boolean] =
    apply(None, templates)

  def apply(
      compiledPackages: ConcurrentCompiledPackages,
      templates: ImmArray[Identifier]): Result[Boolean] =
    apply(Some(compiledPackages), templates)
}
