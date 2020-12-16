// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast

private[validation] class World(packages: PartialFunction[PackageId, Ast.GenPackage[_]]) {

  def lookupPackage(ctx: => Context, pkgId: PackageId): Ast.GenPackage[_] =
    packages.lift(pkgId).getOrElse(throw EUnknownDefinition(ctx, LEPackage(pkgId)))

  def lookupModule(ctx: => Context, pkgId: PackageId, modName: ModuleName): Ast.GenModule[_] =
    lookupPackage(ctx, pkgId).modules
      .getOrElse(modName, throw EUnknownDefinition(ctx, LEModule(pkgId, modName)))

  def lookupDefinition(ctx: => Context, name: TypeConName): Ast.GenDefinition[_] =
    lookupModule(ctx, name.packageId, name.qualifiedName.module).definitions
      .getOrElse(name.qualifiedName.name, throw EUnknownDefinition(ctx, LEDataType(name)))

  def lookupTypeSyn(ctx: => Context, name: TypeSynName): Ast.DTypeSyn =
    lookupDefinition(ctx, name) match {
      case typeSyn: Ast.DTypeSyn =>
        typeSyn
      case _ =>
        throw EUnknownDefinition(ctx, LETypeSyn(name))
    }

  def lookupDataType(ctx: => Context, name: TypeConName): Ast.DDataType =
    lookupDefinition(ctx, name) match {
      case dataType: Ast.DDataType =>
        dataType
      case _ =>
        throw EUnknownDefinition(ctx, LEDataType(name))
    }

  def lookupTemplate(ctx: => Context, name: TypeConName): Ast.GenTemplate[_] =
    lookupModule(ctx, name.packageId, name.qualifiedName.module).templates
      .getOrElse(name.qualifiedName.name, throw EUnknownDefinition(ctx, LETemplate(name)))

  def lookupChoice(
      ctx: => Context,
      tmpName: TypeConName,
      chName: ChoiceName): Ast.GenTemplateChoice[_] =
    lookupTemplate(ctx, tmpName).choices
      .getOrElse(chName, throw EUnknownDefinition(ctx, LEChoice(tmpName, chName)))

  def lookupValue(ctx: => Context, name: ValueRef): Ast.GenDValue[_] =
    lookupDefinition(ctx, name) match {
      case valueDef: Ast.GenDValue[_] =>
        valueDef
      case _ =>
        throw EUnknownDefinition(ctx, LEValue(name))
    }

}
