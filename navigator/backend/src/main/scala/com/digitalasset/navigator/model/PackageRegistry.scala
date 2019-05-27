// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model

import com.digitalasset.navigator.{model => Model}
import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.daml.lf.{iface => DamlLfIface}
import com.digitalasset.daml.lf.data.{Ref => DamlLfRef}

/** Manages a set of known DAML-LF packages. */
case class PackageRegistry(
    private val packages: Map[DamlLfRef.PackageId, DamlLfPackage] = Map.empty,
    private val templates: Map[DamlLfIdentifier, Template] = Map.empty,
    private val typeDefs: Map[DamlLfIdentifier, DamlLfDefDataType] = Map.empty
) {
  private[this] def template(
      packageId: DamlLfRef.PackageId,
      qname: DamlLfQualifiedName,
      r: DamlLfRecord,
      t: DamlLfIface.DefTemplate[DamlLfIface.Type]
  ): Template = Template(
    DamlLfIdentifier(packageId, qname),
    t.choices.toList.map(c => choice(c._1, c._2))
  )

  private[this] def choice(
      name: String,
      c: DamlLfIface.TemplateChoice[DamlLfIface.Type]): Model.Choice = Model.Choice(
    ApiTypes.Choice(name),
    c.param,
    c.returnType,
    c.consuming
  )

  def withPackages(interfaces: List[DamlLfIface.Interface]): PackageRegistry = {
    val newPackages: Map[DamlLfRef.PackageId, DamlLfPackage] = interfaces
      .filterNot(p => packages.contains(p.packageId))
      .map(p => {
        val typeDefs = p.typeDecls.collect {
          case (qname, DamlLfIface.reader.InterfaceType.Normal(t)) =>
            DamlLfIdentifier(p.packageId, qname) -> t
          case (qname, DamlLfIface.reader.InterfaceType.Template(r, _)) =>
            DamlLfIdentifier(p.packageId, qname) -> DamlLfDefDataType(DamlLfImmArraySeq.empty, r)
        }
        val templates = p.typeDecls.collect {
          case (qname, DamlLfIface.reader.InterfaceType.Template(r, t)) =>
            DamlLfIdentifier(p.packageId, qname) -> template(p.packageId, qname, r, t)
        }
        p.packageId -> DamlLfPackage(p.packageId, typeDefs, templates)
      })
      .toMap

    val newTemplates = newPackages
      .map(_._2.templates)
      .reduce(_ ++ _)

    val newTypeDefs = newPackages
      .map(_._2.typeDefs)
      .reduce(_ ++ _)

    copy(
      packages = packages ++ newPackages,
      templates = templates ++ newTemplates,
      typeDefs = typeDefs ++ newTypeDefs
    )
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Packages
  // ------------------------------------------------------------------------------------------------------------------

  def packageCount: Int =
    packages.size

  def pack(id: DamlLfRef.PackageId): Option[DamlLfPackage] =
    packages.get(id)

  def allPackages(): Stream[DamlLfPackage] =
    packages.values.toStream

  // ------------------------------------------------------------------------------------------------------------------
  // Templates
  // ------------------------------------------------------------------------------------------------------------------

  def template(id: DamlLfIdentifier): Option[Template] =
    templates.get(id)

  def templateByIdentifier(id: ApiTypes.TemplateId): Option[Template] = {
    val damlId = ApiTypes.TemplateId.unwrap(id).asDaml
    templates.get(damlId)
  }

  def templateByStringId(id: TemplateStringId): Option[Template] = {
    parseOpaqueIdentifier(TemplateStringId.unwrap(id)).flatMap(lfid => templates.get(lfid))
  }

  def templateCount: Int =
    templates.size

  def allTemplates(): Stream[Template] =
    templates.values.toStream

  def templatesByName(topLevelDecl: String): Seq[Template] =
    templates.toList
      .filter(t => t._2.topLevelDecl == topLevelDecl)
      .map(_._2)

  // ------------------------------------------------------------------------------------------------------------------
  // Types
  // ------------------------------------------------------------------------------------------------------------------

  def damlLfDefDataType(id: DamlLfIdentifier): Option[DamlLfDefDataType] =
    typeDefs.get(id)

  /**
    * Returns a list of all user defined types required to evaluate the given user defined type.
    * maxDepth defines the maximum depth of instantiate() calls (i.e., recursive type lookups)
    */
  def typeDependencies(
      typ: DamlLfDefDataType,
      maxDepth: Int = Int.MaxValue): Map[DamlLfIdentifier, DamlLfDefDataType] = {
    def foldType(
        typ: DamlLfType,
        deps: Map[DamlLfIdentifier, DamlLfDefDataType],
        instantiatesRemaining: Int
    ): Map[DamlLfIdentifier, DamlLfDefDataType] = {
      typ match {
        case t @ DamlLfTypeVar(_) => deps
        case t @ DamlLfTypePrim(_, vars) =>
          vars.foldLeft(deps)((r, v) => foldType(v, r, instantiatesRemaining))
        case t @ DamlLfTypeCon(name, vars) =>
          deps.get(t.name.identifier) match {
            // Dependency already added
            case Some(_) => deps
            // New dependency
            case None =>
              if (instantiatesRemaining > 0) {
                damlLfDefDataType(name.identifier).fold(deps)(ddt => {
                  val r1 = deps + (name.identifier -> ddt)
                  val r2 = foldDataType(ddt, r1, instantiatesRemaining - 1)
                  vars.foldLeft(r2)((r, v) => foldType(v, r, instantiatesRemaining - 1))
                })
              } else {
                deps
              }
          }
      }
    }

    def foldDataType(
        ddt: DamlLfDefDataType,
        deps: Map[DamlLfIdentifier, DamlLfDefDataType],
        instantiatesRemaining: Int
    ): Map[DamlLfIdentifier, DamlLfDefDataType] = {
      ddt.dataType match {
        case DamlLfRecord(fields) =>
          fields.foldLeft(deps)((r, field) => foldType(field._2, r, instantiatesRemaining))
        case DamlLfVariant(fields) =>
          fields.foldLeft(deps)((r, field) => foldType(field._2, r, instantiatesRemaining))
        case DamlLfEnum(_) =>
          // FixMe (RH) https://github.com/digital-asset/daml/issues/105
          throw new NotImplementedError()
      }
    }

    foldDataType(typ, Map.empty, maxDepth)
  }
}
