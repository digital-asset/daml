// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import scalaz.std.tuple._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.boolean._

import scala.collection.immutable.{Map, SeqOps}
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
    metadata: Option[PackageMetadata],
    typeDecls: Map[QualifiedName, PackageSignature.TypeDecl],
    @deprecatedName("astInterfaces", "2.4.0") interfaces: Map[QualifiedName, DefInterface.FWT],
) {
  import PackageSignature.TypeDecl

  @deprecated("renamed to interfaces", since = "2.4.0")
  def astInterfaces: interfaces.type = interfaces
  @deprecated("renamed to getInterfaces", since = "2.4.0")
  def getAstInterfaces: j.Map[QualifiedName, DefInterface.FWT] = getInterfaces

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

  /** Update internal templates, as well as external templates via `setTemplates`,
    * with retroactive interface implementations.
    *
    * @param setTemplate Used to look up templates that can't be found in this
    *                    interface
    */
  private def resolveRetroImplements[S](
      s: S
  )(setTemplate: SetterAt[Ref.TypeConName, S, DefTemplate.FWT]): (S, PackageSignature) = {
    type SandTpls = (S, Map[QualifiedName, TypeDecl.Template])
    def setTpl(
        sm: SandTpls,
        tcn: Ref.TypeConName,
    ): Option[(DefTemplate.FWT => DefTemplate.FWT) => SandTpls] = {
      import PackageSignature.findTemplate
      val (s, tplsM) = sm
      if (tcn.packageId == packageId)
        findTemplate(tplsM orElse typeDecls, tcn.qualifiedName).map {
          case itt @ TypeDecl.Template(_, dt) =>
            f => (s, tplsM.updated(tcn.qualifiedName, itt.copy(template = f(dt))))
        }
      else setTemplate(s, tcn) map (_ andThen ((_, tplsM)))
    }

    val ((sEnd, newTpls), newIfcs) = interfaces.foldLeft(
      ((s, Map.empty): SandTpls, Map.empty[QualifiedName, DefInterface.FWT])
    ) { case ((s, astIfs), (ifcName, astIf)) =>
      astIf
        .resolveRetroImplements(Ref.TypeConName(packageId, ifcName), s)(setTpl)
        .rightMap(newIf => astIfs.updated(ifcName, newIf))
    }
    (sEnd, copy(typeDecls = typeDecls ++ newTpls, interfaces = newIfcs))
  }

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

  // Given a lookup function for package state setters, produce a lookup function
  // for setters on specific templates in that set of packages.
  private[this] def setPackageTemplates[S](
      findPackage: GetterSetterAt[PackageId, S, PackageSignature]
  ): SetterAt[Ref.TypeConName, S, DefTemplate.FWT] = {
    def go(s: S, tcn: Ref.TypeConName): Option[(DefTemplate.FWT => DefTemplate.FWT) => S] = for {
      foundPkg <- findPackage(s, tcn.packageId)
      (ifc, sIfc) = foundPkg
      itt <- findTemplate(ifc.typeDecls, tcn.qualifiedName)
    } yield f =>
      sIfc(
        ifc.copy(typeDecls =
          ifc.typeDecls.updated(tcn.qualifiedName, itt.copy(template = f(itt.template)))
        )
      )
    go
  }

  /** Extend the set of interfaces represented by `s` and `findPackage` with
    * `newSignatures`.  Produce the resulting `S` and a replacement copy of
    * `newSignatures` with templates and interfaces therein resolved.
    *
    * Does not search members of `s` for fresh interfaces.
    */
  def resolveRetroImplements[S, CC[B] <: Seq[B] with SeqOps[B, CC, CC[B]]](
      s: S,
      @deprecatedName("newInterfaces", "2.4.0") newSignatures: CC[PackageSignature],
  )(
      findPackage: GetterSetterAt[PackageId, S, PackageSignature]
  ): (S, CC[PackageSignature]) = {
    type St = (S, CC[PackageSignature])
    val findTpl = setPackageTemplates[St] { case ((s, newSignatures), pkgId) =>
      findPackage(s, pkgId).map(_.rightMap(_ andThen ((_, newSignatures)))).orElse {
        val ix = newSignatures indexWhere (_.packageId == pkgId)
        (ix >= 0) option ((newSignatures(ix), newSig => (s, newSignatures.updated(ix, newSig))))
      }
    }

    (0 until newSignatures.size).foldLeft((s, newSignatures)) {
      case (st @ (_, newSignatures), ifcK) =>
        val ((s2, newSignatures2), newAtIfcK) =
          newSignatures(ifcK).resolveRetroImplements(st)(findTpl)
        // the tricky part here: newSignatures2 is guaranteed not to have altered
        // the value at ifcK, and to have made all "self" changes in newAtIfcK.
        // So there is no conflict, we can discard the value in the seq
        (s2, newSignatures2.updated(ifcK, newAtIfcK))
    }
  }

  @deprecated("renamed to findInterface", since = "2.4.0")
  def findAstInterface(
      findPackage: PartialFunction[PackageId, PackageSignature]
  ): PartialFunction[Ref.TypeConName, DefInterface.FWT] =
    findInterface(findPackage)

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
      @deprecatedName("findInterface", "2.4.0") findPackage: PartialFunction[
        PackageId,
        PackageSignature,
      ]
  ): PartialFunction[Ref.TypeConName, DefInterface.ViewTypeFWT] =
    Function unlift { tcn =>
      findPackage.lift(tcn.packageId) flatMap (_ resolveInterfaceViewType tcn.qualifiedName)
    }

}
