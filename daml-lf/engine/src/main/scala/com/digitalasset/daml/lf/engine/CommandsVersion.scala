// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.transaction.VersionTimeline
import com.digitalasset.daml.lf.language.{LanguageVersion, LanguageMajorVersion => LMV}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data.{ImmArray, ImmArrayCons}
import com.digitalasset.daml.lf.language.Ast.Package

import scala.annotation.tailrec

/** Some DAML-LF operations (curently contract key lookup and fetch) are parametrized
  * by the language version that "originated" the transactions.
  *
  * For scenarios we just use the version of the module where the scenario
  * definition comes from. For Ledger API commands, we use the latest version
  * amongst the versions of the modules from where the templates of the commands
  * come from.
  *
  * We only return [[ResultError]], [[ResultDone]], and [[ResultNeedPackage]].
  */
object CommandsVersion {
  private def templateVersion(
      compiledPackages: Option[ConcurrentCompiledPackages],
      templateId: Identifier): Result[LanguageVersion] = {
    def withPkg(pkg: Package): Result[LanguageVersion] =
      pkg.modules.get(templateId.qualifiedName.module) match {
        case None => ResultError(Error(s"Could not find module ${templateId.qualifiedName.module}"))
        case Some(module) => ResultDone(module.languageVersion)
      }

    compiledPackages match {
      case None => Result.needPackage(templateId.packageId, withPkg)
      case Some(pkgs) => Result.needPackage(pkgs, templateId.packageId, withPkg)
    }
  }

  private def apply(
      compiledPackages: Option[ConcurrentCompiledPackages],
      templates0: ImmArray[Identifier]): Result[LanguageVersion] = {
    // not using [[Result#sequence]] on purpose, see
    // <https://github.com/digital-asset/daml/blob/995ee82fd0655231d7034d0a66c9fe2c6a419536/daml-lf/engine/src/main/scala/com/digitalasset/daml/lf/engine/CommandPreprocessor.scala#L463>
    @tailrec
    def go(
        currentVersion: LanguageVersion,
        templates: ImmArray[Identifier]): Result[LanguageVersion] = {
      templates match {
        case ImmArray() => ResultDone(currentVersion)
        case ImmArrayCons(template, rest) =>
          templateVersion(compiledPackages, template) match {
            case ResultError(err) => ResultError(err)
            case ResultDone(templateVersion) =>
              go(VersionTimeline.maxVersion(currentVersion, templateVersion), rest)
            case ResultNeedPackage(pkgId, resume) =>
              ResultNeedPackage(pkgId, { pkg =>
                resume(pkg).flatMap { templateVersion =>
                  goResume(VersionTimeline.maxVersion(currentVersion, templateVersion), rest)
                }
              })
            case result => sys.error(s"Unexpected result: $result")
          }
      }
    }
    def goResume(
        currentVersion: LanguageVersion,
        templates: ImmArray[Identifier]): Result[LanguageVersion] =
      go(currentVersion, templates)
    // for no commands the version is irrelevant -- we just return
    // the earliest one.
    go(LanguageVersion(LMV.V1, LMV.V1.acceptedVersions.head), templates0)
  }

  def apply(commands: Commands): Result[LanguageVersion] =
    apply(None, commands.commands.map(_.templateId))

  def apply(
      compiledPackages: ConcurrentCompiledPackages,
      commands: Commands): Result[LanguageVersion] =
    apply(Some(compiledPackages), commands.commands.map(_.templateId))

  def apply(templates: ImmArray[Identifier]): Result[LanguageVersion] =
    apply(None, templates)

  def apply(
      compiledPackages: ConcurrentCompiledPackages,
      templates: ImmArray[Identifier]): Result[LanguageVersion] =
    apply(Some(compiledPackages), templates)
}
