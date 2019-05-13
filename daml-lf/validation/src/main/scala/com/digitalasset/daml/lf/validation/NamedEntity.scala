// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.Ref.{DottedName, ModuleName, Name}
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.validation.Util._

sealed trait NamedEntity extends Product with Serializable {
  def modName: ModuleName
  def fullyResolvedName: DottedName
  def pretty: String
}

object NamedEntity {

  final case class NModDef(
      name: ModuleName,
      dfns: List[(DottedName, Ast.Definition)]
  ) extends NamedEntity {

    def modName: ModuleName = name

    def fullyResolvedName: DottedName = name.toUpperCase

    override def toString = s"NModDef($name)"

    def pretty: String = s"module $name"
  }

  final case class NValDef(
      module: NModDef,
      name: DottedName,
      dfn: Ast.DValue,
  ) extends NamedEntity {

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      module.fullyResolvedName ++ name.toUpperCase

    override def toString: String = s"NValDef($modName:$name)"

    def pretty: String = s"value $modName:$name"
  }

  final case class NRecDef(
      module: NModDef,
      name: DottedName,
      dfn: Ast.DDataType
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
      name: Name
  ) extends NamedEntity {

    def module: NModDef = dfn.module

    def modName: ModuleName = module.name

    val fullyResolvedName: DottedName =
      dfn.fullyResolvedName + Name.assertFromString(name.toUpperCase)

    override def toString: String = s"NField($modName:${dfn.name}:$name)"

    def pretty: String = s"record field $modName:${dfn.name}:$name"
  }

}
