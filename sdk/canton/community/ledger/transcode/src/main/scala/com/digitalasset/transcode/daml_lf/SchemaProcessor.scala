// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.daml_lf

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast.GenDefInterface
import com.digitalasset.daml.lf.language.{Ast, Util}
import com.digitalasset.transcode.daml_lf.SchemaEntity.PackageInfo
import com.digitalasset.transcode.daml_lf.SchemaEntity.Template.DataKind
import com.digitalasset.transcode.schema.*

import scala.Function.const
import scala.collection.mutable
import scala.util.Try

/** [[SchemaProcessor]] traverses Daml Schema and produces results by combining
  * [[com.digitalasset.transcode.schema.SchemaVisitor]] and Entity Collector ([[CollectResult]]).
  */
object SchemaProcessor {

  /** Process Daml Packages. Provide visitor (codec/codegen) and result collector
    * ([[Dictionary.collect]] etc).
    */
  def process[T](
      packages: Map[Ref.PackageId, Ast.PackageSignature],
      filter: ((Ref.PackageName, Ref.QualifiedName)) => Boolean = const(true),
  )(visitor: SchemaVisitor)(
      collect: CollectResult[visitor.Type, T]
  ): Either[String, T] =
    process(packages.keys, packages.apply, filter)(visitor)(collect)

  /** Process Daml Packages. Package signatures are computed lazily on demand. Provide visitor
    * (codec/codegen) and result collector ([[Dictionary.collect]] etc).
    */
  def process[T](
      rootPackages: IterableOnce[Ref.PackageId],
      getPackageSignature: Ref.PackageId => Ast.PackageSignature,
      filter: ((Ref.PackageName, Ref.QualifiedName)) => Boolean,
  )(visitor: SchemaVisitor)(
      collect: CollectResult[visitor.Type, T]
  ): Either[String, T] =
    Try {
      collect(
        new SchemaProcessor[visitor.Type](
          rootPackages,
          getPackageSignature,
          filter,
          visitor,
        ).getEntities
      )
    }.toEither.left.map { error =>
      error.printStackTrace()
      error.getMessage
    }
}

// warning reason: code is translated from scala3
@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
private class SchemaProcessor[T](
    rootPackages: IterableOnce[Ref.PackageId],
    getPackageSignature: Ref.PackageId => Ast.PackageSignature,
    filter: ((Ref.PackageName, Ref.QualifiedName)) => Boolean,
    visitor: SchemaVisitor { type Type = T },
) {

  def getEntities: Seq[SchemaEntity[T]] = {
    require(
      apiDefinitions.nonEmpty,
      "No user-supplied Daml models found on connected ledger. Please, deploy your application's DAR to the ledger.",
    )
    apiDefinitions
  }

  private type Args = Seq[visitor.Type]
  private type VarMap = Seq[(TypeVarName, visitor.Type)]

  private def err(msg: String) = throw new RuntimeException(msg)

  private val getPackage = cached { (pkgId: Ref.PackageId) =>
    getPackageSignature(pkgId)
  }

  private val getInterface = cached { (id: Ref.Identifier) =>
    val interface =
      getPackage(id.packageId).modules(id.qualifiedName.module).interfaces(id.qualifiedName.name)
    require(
      interface.coImplements.isEmpty,
      s"$id contains coImplements definition, which is not supported. Please remove it.",
    )
    interface
  }

  private val getPackageInfo = cached { (id: Ref.PackageId) =>
    val metadata = getPackage(id).metadata
    PackageInfo(metadata.name, metadata.version)
  }

  private val getIdentifier = { (id: Ref.Identifier, packageName: Ref.PackageName) =>
    Identifier(
      packageId = id.packageId,
      packageName = packageName,
      moduleName = id.qualifiedName.module.dottedName,
      entityName = id.qualifiedName.name.dottedName,
    )
  }

  private val getDefinition = cached { (id: Ref.Identifier) =>
    val pkg = getPackage(id.packageId)
    val module = pkg.modules(id.qualifiedName.module)
    if (module.definitions.contains(id.qualifiedName.name)) {
      module.definitions(id.qualifiedName.name) -> pkg.metadata.name
    } else {
      module.interfaces(id.qualifiedName.name) -> pkg.metadata.name
    }
  }

  // All top-level API definitions (Templates, Choices, Interfaces)
  private lazy val apiDefinitions: Seq[SchemaEntity[visitor.Type]] = for {
    pkgId <- rootPackages.iterator.toSeq
    pkg = getPackage(pkgId)
    (moduleName, module) <- pkg.modules
    (templateName, template) <- module.templates

    templateId = Ref.Identifier(pkgId, Ref.QualifiedName(moduleName, templateName))
    interfaceIds = template.implements.keys
    (included, excluded) = (templateId +: interfaceIds)
      .map(id => getPackageInfo(id.packageId).name -> id.qualifiedName)
      .partition(filter)

    // if any of interfaces or base classes are included, then the whole hierarchy is included
    // orphan interfaces or interfaces with coImplement definitions are not included
    pretty = (names: Seq[(Ref.PackageName, Ref.QualifiedName)]) =>
      names.map { case (p, n) => s"$p:$n" }.mkString(", ")
    _ = require(
      included.nonEmpty && excluded.isEmpty || included.isEmpty,
      s"Expected [${pretty(excluded)}] to be included along with [${pretty(included)}], but former were excluded",
    ) if included.nonEmpty

    templateEntity = SchemaEntity.Template(
      templateId,
      getPackageInfo(templateId.packageId),
      fromDef((templateId, Seq.empty)),
      template.key.map(x => fromType(x.typ, Seq.empty)),
      DataKind.Template,
      template.implements.values.map(_.interfaceId).toSeq,
    )

    choiceEntities = template.choices.map { case (name, choice) =>
      SchemaEntity.Choice(
        templateId,
        getPackageInfo(templateId.packageId),
        choice.name,
        fromType(choice.argBinder._2, Seq.empty),
        fromType(choice.returnType, Seq.empty),
        choice.consuming,
      )
    }

    interfaceEntities = template.implements.keys
      .map(id => id -> getInterface(id))
      .flatMap { case (id, i) =>
        val interfaceEntity = SchemaEntity.Template(
          id,
          getPackageInfo(id.packageId),
          fromType(i.view, Seq.empty),
          None,
          DataKind.Interface,
          Seq.empty,
        )
        val choiceEntities = i.choices.map { case (name, choice) =>
          SchemaEntity.Choice(
            id,
            getPackageInfo(id.packageId),
            choice.name,
            fromType(choice.argBinder._2, Seq.empty),
            fromType(choice.returnType, Seq.empty),
            choice.consuming,
          )
        }.toSeq
        Seq(interfaceEntity) ++ choiceEntities
      }
    entity <- Seq(templateEntity) ++ choiceEntities ++ interfaceEntities
  } yield entity

  // Addressable types that have FQNames
  private val fromDef = cached[(Ref.Identifier, Args), visitor.Type] { case (id, args) =>
    getDefinition(id) match {
      case (Ast.DDataType(_, params, cons), pkgName) =>
        fromCons(id, pkgName, cons, params.toSeq.map { case (n, _) => TypeVarName(n) } zip args)
      case (Ast.DTypeSyn(params, typ), _) =>
        fromType(typ, params.toSeq.map { case (n, _) => TypeVarName(n) } zip args)
      case (iface: GenDefInterface[_], _) => fromType(iface.view, Seq.empty)
      case (other, _) => err(s"Data type $other is not supported")
    }
  }

  // Records, Variants and Enums
  private def fromCons(
      id: Ref.Identifier,
      pkgName: Ref.PackageName,
      cons: Ast.DataCons,
      varMap: VarMap,
  ): visitor.Type =
    cons match {
      case Ast.DataRecord(fields) =>
        lazy val fieldProcessors = fields.toSeq.map { case (name, typ) =>
          FieldName(name) -> fromType(typ, varMap)
        }
        visitor.record(getIdentifier(id, pkgName), varMap, fieldProcessors)

      case Ast.DataVariant(variants) =>
        lazy val cases = variants.toSeq.map { case (name, typ) =>
          VariantConName(name) -> fromType(typ, varMap)
        }
        visitor.variant(getIdentifier(id, pkgName), varMap, cases)

      case Ast.DataEnum(constructors) =>
        visitor.`enum`(getIdentifier(id, pkgName), constructors.map(EnumConName).toSeq)

      case Ast.DataInterface =>
        visitor.interface(getIdentifier(id, pkgName))
    }

  // Simple types and type applications
  private def fromType(typ: Ast.Type, varMap: VarMap): visitor.Type = typ match {
    case Util.TTyConApp(id, args) => fromDef((id, args.toSeq.map(fromType(_, varMap))))
    case Ast.TSynApp(id, args) => fromDef((id, args.toSeq.map(fromType(_, varMap))))

    case Ast.TVar(name) =>
      visitor.variable(
        TypeVarName(name),
        varMap
          .collectFirst { case (n, p) if n == TypeVarName(name) => p }
          .getOrElse(err(s"Variable $name not found")),
      )

    case Util.TList(typ) => visitor.list(fromType(typ, varMap))
    case Util.TOptional(typ) => visitor.optional(fromType(typ, varMap))
    case Util.TTextMap(typ) => visitor.textMap(fromType(typ, varMap))
    case Util.TGenMap(kTyp, vTyp) => visitor.genMap(fromType(kTyp, varMap), fromType(vTyp, varMap))

    case Util.TUnit => visitor.unit
    case Util.TBool => visitor.bool
    case Util.TText => visitor.text
    case Util.TInt64 => visitor.int64
    case Util.TNumeric(Ast.TNat(scale)) => visitor.numeric(scale)
    case Util.TTimestamp => visitor.timestamp
    case Util.TDate => visitor.date
    case Util.TParty => visitor.party
    case Util.TContractId(typ) => visitor.contractId(fromType(typ, varMap))

    case other => err(s"Type $other not supported")
  }

  private def cached[K, V](compute: K => V): K => V = {
    val cache = mutable.HashMap.empty[K, V]
    (k: K) => cache.getOrElseUpdate(k, compute(k))
  }
}
