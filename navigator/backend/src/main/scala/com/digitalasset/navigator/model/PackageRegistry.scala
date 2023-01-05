// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import com.daml.navigator.{model => Model}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.lf.{typesig => DamlLfIface}
import com.daml.lf.data.{Ref => DamlLfRef}

/** Manages a set of known Daml-LF packages. */
case class PackageRegistry(
    private val packageState: PackageState = PackageState(Map.empty),

    // These are just projections from `packageState` for performance. `packageState` is the source of truth
    private val packages: Map[DamlLfRef.PackageId, DamlLfPackage] = Map.empty,
    private val templates: Map[DamlLfIdentifier, Template] = Map.empty,
    private val typeDefs: Map[DamlLfIdentifier, DamlLfDefDataType] = Map.empty,
    private val interfaces: Map[DamlLfIdentifier, Interface] = Map.empty,
) {
  // TODO (#13969) ignores inherited choices; interfaces aren't handled at all
  private[this] def template(
      packageId: DamlLfRef.PackageId,
      qname: DamlLfQualifiedName,
      t: DamlLfIface.DefTemplate[DamlLfIface.Type],
  ): Template = Template(
    DamlLfIdentifier(packageId, qname),
    t.tChoices.resolvedChoices.toList.flatMap { case (choiceName, resolvedChoices) =>
      resolvedChoices.map { case (interfaceIdOption, templateChoice) =>
        choice(choiceName, templateChoice, interfaceIdOption)
      }
    },
    t.key,
    t.implementedInterfaces.toSet,
  )

  private[this] def interface(
      packageId: DamlLfRef.PackageId,
      qname: DamlLfQualifiedName,
      interface: DamlLfIface.DefInterface[DamlLfIface.Type],
  ): Interface = {
    Interface(
      DamlLfIdentifier(packageId, qname),
      interface.choices.toList.map(c => choice(c._1, c._2)),
    )
  }

  private[this] def choice(
      name: String,
      c: DamlLfIface.TemplateChoice[DamlLfIface.Type],
      inheritedInterface: Option[DamlLfIdentifier] = None,
  ): Model.Choice = Model.Choice(
    ApiTypes.Choice(name),
    c.param,
    c.returnType,
    c.consuming,
    inheritedInterface,
  )

  def withPackages(interfaces: List[DamlLfIface.PackageSignature]): PackageRegistry = {
    val newPackageStore = packageState.append(interfaces.map(p => p.packageId -> p).toMap)

    val newPackages = newPackageStore.packages.values.map { p =>
      val typeDefs = p.typeDecls.map { case (qname, td) =>
        DamlLfIdentifier(p.packageId, qname) -> td.`type`
      }
      val templates = p.typeDecls.collect {
        case (qname, DamlLfIface.PackageSignature.TypeDecl.Template(r @ _, t)) =>
          DamlLfIdentifier(p.packageId, qname) -> template(p.packageId, qname, t)
      }
      val interfaces = p.interfaces.map { case (qname, defInterface) =>
        DamlLfIdentifier(p.packageId, qname) -> interface(p.packageId, qname, defInterface)
      }
      p.packageId -> DamlLfPackage(p.packageId, typeDefs, templates, interfaces)
    }.toMap

    copy(
      packages = newPackages,
      templates = newPackages.flatMap(_._2.templates),
      typeDefs = newPackages.flatMap(_._2.typeDefs),
      interfaces = newPackages.flatMap(_._2.interfaces),
    )
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Packages
  // ------------------------------------------------------------------------------------------------------------------

  def packageCount: Int =
    packages.size

  def pack(id: DamlLfRef.PackageId): Option[DamlLfPackage] =
    packages.get(id)

  def allPackages(): LazyList[DamlLfPackage] =
    packages.values.to(LazyList)

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

  def allTemplates(): LazyList[Template] =
    templates.values.to(LazyList)

  def templatesByName(topLevelDecl: String): Seq[Template] =
    templates.toList
      .filter(t => t._2.topLevelDecl == topLevelDecl)
      .map(_._2)

  // ------------------------------------------------------------------------------------------------------------------
  // Types
  // ------------------------------------------------------------------------------------------------------------------

  def damlLfDefDataType(id: DamlLfIdentifier): Option[DamlLfDefDataType] =
    typeDefs.get(id)

  /** Returns a list of all user defined types required to evaluate the given user defined type.
    * maxDepth defines the maximum depth of instantiate() calls (i.e., recursive type lookups)
    */
  def typeDependencies(
      typ: DamlLfDefDataType,
      maxDepth: Int = Int.MaxValue,
  ): Map[DamlLfIdentifier, DamlLfDefDataType] = {
    def foldType(
        typ: DamlLfType,
        deps: Map[DamlLfIdentifier, DamlLfDefDataType],
        instantiatesRemaining: Int,
    ): Map[DamlLfIdentifier, DamlLfDefDataType] = {
      typ match {
        case DamlLfTypeVar(_) | DamlLfTypeNumeric(_) => deps
        case DamlLfTypePrim(_, vars) =>
          vars.foldLeft(deps)((r, v) => foldType(v, r, instantiatesRemaining))
        case DamlLfTypeCon(name, vars) =>
          deps.get(name.identifier) match {
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
        instantiatesRemaining: Int,
    ): Map[DamlLfIdentifier, DamlLfDefDataType] = {
      ddt.dataType match {
        case DamlLfRecord(fields) =>
          fields.foldLeft(deps)((r, field) => foldType(field._2, r, instantiatesRemaining))
        case DamlLfVariant(fields) =>
          fields.foldLeft(deps)((r, field) => foldType(field._2, r, instantiatesRemaining))
        case DamlLfEnum(_) =>
          deps
      }
    }

    foldDataType(typ, Map.empty, maxDepth)
  }
}
