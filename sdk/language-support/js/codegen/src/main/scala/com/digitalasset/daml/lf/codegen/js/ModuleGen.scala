// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.Ref.{ModuleName, PackageId}
import com.digitalasset.daml.lf.language.Ast

private[codegen] final case class ModuleGen(
    moduleName: ModuleName,
    scope: String,
    externalImports: Seq[(PackageId, Ast.PackageMetadata)],
    internalImports: Seq[ModuleName],
    definitions: Seq[DefGen],
) {
  private val rootPath = moduleName.segments.toSeq.map(_ => "..").mkString("/")

  def renderJsSource: String = {
    val importSection = allImports
      .map { case (ref, path) => GenHelper.renderES5Import(ref, path) }
      .mkString("\n")
    (Seq(jsHeader, importSection) ++ definitions.map(_.renderJsSource)).mkString("\n\n")
  }

  def renderTsExport: String = {
    val importSection = allImports
      .map { case (ref, path) => GenHelper.renderES6Import(ref, path) }
      .mkString("\n")
    (Seq(tsHeader ++ importSection) ++ definitions.map(_.renderTsExport)).mkString("\n\n")
  }

  private def allImports: Seq[(String, String)] =
    externalImports.map(pkg => (packageVar(pkg._1), packagePath(pkg._2))) ++
      internalImports.map(module => (moduleVar(module), modulePath(module)))

  private def moduleVar(module: ModuleName): String = module.segments.toSeq.mkString("_")
  private def packageVar(pkg: PackageId): String = s"pkg$pkg"

  private def modulePath(module: ModuleName) =
    s"$rootPath/${module.segments.toSeq.mkString("/")}/module"
  private def packagePath(pkg: Ast.PackageMetadata) = s"$scope/${pkg.nameDashVersion}"

  private val jsHeader: String =
    s"""|${GenHelper.commonjsHeader}
        |/* eslint-disable-next-line no-unused-vars */
        |var jtv = require('@mojotech/json-type-validation');
        |/* eslint-disable-next-line no-unused-vars */
        |var damlTypes = require('@daml/types');
        |""".stripMargin

  private val tsHeader: String =
    s"""|// Generated from ${modulePath(moduleName)}.daml
        |/* eslint-disable @typescript-eslint/camelcase */
        |/* eslint-disable @typescript-eslint/no-namespace */
        |/* eslint-disable @typescript-eslint/no-use-before-define */
        |import * as jtv from '@mojotech/json-type-validation';
        |import * as damlTypes from '@daml/types';
        |""".stripMargin
}
