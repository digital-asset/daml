// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast

private[validation] class World(packages: PartialFunction[PackageId, Ast.Package]) {

  def lookupPackage(ctx: => Context, pkgId: PackageId): Ast.Package =
    packages.lift(pkgId).getOrElse(throw EUnknownDefinition(ctx, LEPackage(pkgId)))

  def lookupModule(ctx: => Context, pkgId: PackageId, modName: ModuleName): Ast.Module =
    lookupPackage(ctx, pkgId).modules
      .getOrElse(modName, throw EUnknownDefinition(ctx, LEModule(pkgId, modName)))

  def lookupDefinition(ctx: => Context, name: TypeConName): Ast.Definition =
    lookupModule(ctx, name.packageId, name.qualifiedName.module).definitions
      .getOrElse(name.qualifiedName.name, throw EUnknownDefinition(ctx, LEDataType(name)))

  def lookupDataType(ctx: => Context, name: TypeConName): Ast.DDataType =
    lookupDefinition(ctx, name) match {
      case dataType: Ast.DDataType =>
        dataType
      case _ =>
        throw EUnknownDefinition(ctx, LEDataType(name))
    }

  def lookupTemplate(ctx: => Context, name: TypeConName): Ast.Template =
    lookupDataType(ctx, name) match {
      case Ast.DDataType(_, _, Ast.DataRecord(_, Some(tmpl))) =>
        tmpl
      case _ =>
        throw EUnknownDefinition(ctx, LETemplate(name))
    }

  def lookupChoice(ctx: => Context, tmpName: TypeConName, chName: ChoiceName): Ast.TemplateChoice =
    lookupTemplate(ctx, tmpName).choices
      .getOrElse(chName, throw EUnknownDefinition(ctx, LEChoice(tmpName, chName)))

  def lookupValue(ctx: => Context, name: ValueRef): Ast.DValue =
    lookupDefinition(ctx, name) match {
      case valueDef: Ast.DValue =>
        valueDef
      case _ =>
        throw EUnknownDefinition(ctx, LEValue(name))
    }

}
