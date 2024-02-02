// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.typesig

import java.{util => j}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import Ref.{Identifier, PackageId, PackageName, PackageVersion, QualifiedName}
import reader.Errors
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.ArchivePayload
import scalaz.std.either._
import scalaz.syntax.bifunctor._

import scala.collection.immutable.Map
import scala.jdk.CollectionConverters._

// Duplicate of the one in com.daml.lf.language to separate Ast and Iface
final case class PackageMetadata(
    name: PackageName,
    version: PackageVersion,
)

/** The interface of a single DALF archive.  Not expressive enough to
  * represent a whole dar, as a dar can contain multiple DALF archives
  * with separate package IDs and overlapping [[QualifiedName]]s; for a
  * dar use [[EnvironmentInterface]] instead.
  */
final case class PackageSignature(
    packageId: PackageId,
    metadata: PackageMetadata,
    typeDecls: Map[QualifiedName, PackageSignature.TypeDecl],
    interfaces: Map[QualifiedName, DefInterface.FWT],
) {
  import PackageSignature.TypeDecl

  def getTypeDecls: j.Map[QualifiedName, TypeDecl] = typeDecls.asJava
  def getInterfaces: j.Map[QualifiedName, DefInterface.FWT] = interfaces.asJava

  private def resolveChoices(
      findInterface: PartialFunction[Ref.TypeConName, DefInterface.FWT],
      failIfUnresolvedChoicesLeft: Boolean,
  ): PackageSignature = {
    val outside = findInterface.lift
    def findIface(id: Identifier) =
      if (id.packageId == packageId) interfaces get id.qualifiedName
      else outside(id)
    val tplFindIface = Function unlift findIface
    def transformTemplate(ift: TypeDecl.Template) = {
      val errOrItt = (ift.template resolveChoices tplFindIface)
        .bimap(
          _.map(partial => ift.copy(template = partial)),
          resolved => ift.copy(template = resolved),
        )
      errOrItt.fold(
        e =>
          if (failIfUnresolvedChoicesLeft)
            throw new IllegalStateException(
              s"Couldn't resolve inherited choices ${e.describeError}"
            )
          else e.partialResolution,
        identity,
      )
    }
    copy(typeDecls = typeDecls transform { (_, ift) =>
      ift match {
        case ift: TypeDecl.Template => transformTemplate(ift)
        case n: TypeDecl.Normal => n
      }
    })
  }

  /** Like [[EnvironmentSignature#resolveChoices]], but permits incremental
    * resolution of newly-loaded interfaces, such as json-api does.
    *
    * {{{
    *  // suppose
    *  i: PackageSignature; ei: EnvironmentSignature
    *  val eidelta = EnvironmentSignature.fromReaderInterfaces(i)
    *  // such that
    *  ei |+| eidelta
    *  // contains the whole environment of i.  Then
    *  ei |+| eidelta.resolveChoicesAndFailOnUnresolvableChoices(ei.astInterfaces)
    *  === (ei |+| eidelta).resolveChoices
    *  // but faster.
    * }}}
    */
  def resolveChoicesAndFailOnUnresolvableChoices(
      findInterface: PartialFunction[Ref.TypeConName, DefInterface.FWT]
  ): PackageSignature = resolveChoices(findInterface, failIfUnresolvedChoicesLeft = true)

  /** Like resolveChoicesAndFailOnUnresolvableChoices, but simply discard
    * unresolved choices from the structure.  Not wise to use on a receiver
    * without a complete environment provided as the argument.
    */
  def resolveChoicesAndIgnoreUnresolvedChoices(
      findInterface: PartialFunction[Ref.TypeConName, DefInterface.FWT]
  ): PackageSignature = resolveChoices(findInterface, failIfUnresolvedChoicesLeft = false)

  private def resolveInterfaceViewType(n: Ref.QualifiedName): Option[Record.FWT] =
    typeDecls get n flatMap (_.asInterfaceViewType)
}

object PackageSignature {
  import Errors._
  import reader.SignatureReader._

  sealed abstract class TypeDecl extends Product with Serializable {
    def `type`: DefDataType.FWT

    def fold[Z](normal: DefDataType.FWT => Z, template: (Record.FWT, DefTemplate[Type]) => Z): Z =
      this match {
        case TypeDecl.Normal(typ) => normal(typ)
        case TypeDecl.Template(typ, tpl) => template(typ, tpl)
      }

    /** Alias for `type`. */
    def getType: DefDataType.FWT = `type`
    def getTemplate: j.Optional[_ <: DefTemplate.FWT] =
      fold(
        { _ =>
          j.Optional.empty()
        },
        { (_, tpl) =>
          j.Optional.of(tpl)
        },
      )

    private[typesig] def asInterfaceViewType: Option[DefInterface.ViewTypeFWT] = this match {
      case TypeDecl.Template(r, _) => Some(r)
      case TypeDecl.Normal(DefDataType(_, dt)) =>
        dt match {
          case r @ Record(_) => Some(r)
          case Variant(_) | Enum(_) => None
        }
    }
  }

  object TypeDecl {
    final case class Normal(`type`: DefDataType.FWT) extends TypeDecl
    final case class Template(rec: Record.FWT, template: DefTemplate[Type]) extends TypeDecl {
      def `type`: DefDataType.FWT = DefDataType(ImmArraySeq.empty, rec)
    }
  }

  def read(lf: DamlLf.Archive): (Errors[ErrorLoc, InvalidDataTypeDefinition], PackageSignature) =
    readPackageSignature(lf)

  def read(lf: ArchivePayload): (Errors[ErrorLoc, InvalidDataTypeDefinition], PackageSignature) =
    readPackageSignature(lf)

  private[typesig] def findTemplate[K](
      m: PartialFunction[K, TypeDecl],
      k: K,
  ): Option[TypeDecl.Template] =
    m.lift(k) collect { case itt: TypeDecl.Template => itt }

  /** An argument for [[PackageSignature#resolveChoices]] given a package database,
    * such as json-api's `LedgerReader.PackageStore`.
    */
  def findInterface(
      findPackage: PartialFunction[PackageId, PackageSignature]
  ): PartialFunction[Ref.TypeConName, DefInterface.FWT] = {
    val pkg = findPackage.lift
    def go(id: Identifier) = pkg(id.packageId).flatMap(_.interfaces get id.qualifiedName)
    Function unlift go
  }

  /** Given a database of interfaces, return a function that will match a
    * [[DefInterface#viewType]] with its record definition if present.
    * The function will not match if the definition is missing or is not a record.
    */
  def resolveInterfaceViewType(
      findPackage: PartialFunction[PackageId, PackageSignature]
  ): PartialFunction[Ref.TypeConName, DefInterface.ViewTypeFWT] =
    Function unlift { tcn =>
      findPackage.lift(tcn.packageId) flatMap (_ resolveInterfaceViewType tcn.qualifiedName)
    }

}
