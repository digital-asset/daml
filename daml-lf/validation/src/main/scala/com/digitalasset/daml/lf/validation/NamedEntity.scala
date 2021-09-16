// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{DottedName, ModuleName, Name}
import com.daml.lf.language.Ast
import com.daml.lf.validation.Util._

sealed trait NamedEntity extends Product with Serializable {
  def modName: ModuleName
  def fullyResolvedName: DottedName
  def pretty: String
}

object NamedEntity {

  final case class NModDef(
      name: ModuleName,
      dfns: List[(DottedName, Ast.Definition)],
  ) extends NamedEntity {

    def modName: ModuleName = name

    def fullyResolvedName: DottedName = name.toUpperCase

    override def toString = s"NModDef($name)"

    def pretty: String = s"module $name"
  }

  final case class NRecDef(
      module: NModDef,
      name: DottedName,
      dfn: Ast.DDataType,
  ) extends NamedEntity {

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      module.fullyResolvedName ++ name.toUpperCase

    override def toString: String = s"NRecDef($modName:$name)"

    def pretty: String = s"record $modName:$name"
  }

  final case class NVarDef(
      module: NModDef,
      name: DottedName,
      dfn: Ast.DDataType,
  ) extends NamedEntity {

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      module.fullyResolvedName ++ name.toUpperCase

    override def toString: String = s"NVarDef($modName:$name)"

    def pretty: String = s"variant $modName:$name"
  }

  final case class NEnumDef(
      module: NModDef,
      name: DottedName,
      dfn: Ast.DDataType,
  ) extends NamedEntity {

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      module.fullyResolvedName ++ name.toUpperCase

    override def toString: String = s"NEnumDef($modName:$name)"

    def pretty: String = s"enum $modName:$name"
  }

  final case class NSynDef(
      module: NModDef,
      name: DottedName,
  ) extends NamedEntity {
    override def modName: ModuleName = module.name

    override def fullyResolvedName: Ref.ModuleName =
      module.fullyResolvedName ++ name.toUpperCase

    override def toString: String = s"NSynDef($modName:$name)"

    override def pretty: String = s"type synonym $modName:$name"
  }

  final case class NVarCon(
      dfn: NVarDef,
      name: Name,
  ) extends NamedEntity {

    def module: NModDef = dfn.module

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      dfn.fullyResolvedName + Name.assertFromString(name.toUpperCase)

    override def toString: String = s"NVarCon($modName:${dfn.name}:$name)"

    def pretty: String = s"variant constructor $modName:${dfn.name}:$name"
  }

  final case class NField(
      dfn: NRecDef,
      name: Name,
  ) extends NamedEntity {

    def module: NModDef = dfn.module

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      dfn.fullyResolvedName + Name.assertFromString(name.toUpperCase)

    override def toString: String = s"NField($modName:${dfn.name}:$name)"

    def pretty: String = s"record field $modName:${dfn.name}:$name"
  }

  final case class NEnumCon(
      dfn: NEnumDef,
      name: Name,
  ) extends NamedEntity {

    def module: NModDef = dfn.module

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      dfn.fullyResolvedName + Name.assertFromString(name.toUpperCase)

    override def toString: String = s"NVarCon($modName:${dfn.name}:$name)"

    def pretty: String = s"variant constructor $modName:${dfn.name}:$name"
  }

  final case class NInterface(
      module: NModDef,
      name: DottedName,
      dfn: Ast.DDataType,
  ) extends NamedEntity {

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      module.fullyResolvedName ++ name.toUpperCase

    override def toString: String = s"NInterface($modName:$name)"

    def pretty: String = s"interface $modName:$name"
  }
}
