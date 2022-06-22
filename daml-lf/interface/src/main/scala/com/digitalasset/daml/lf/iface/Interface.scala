// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.iface

import java.{util => j}

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref, Ref.{Identifier, PackageId, PackageName, PackageVersion, QualifiedName}
import com.daml.lf.iface.reader.Errors
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.ArchivePayload

import scalaz.std.either._
import scalaz.std.tuple._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.boolean._

import scala.collection.immutable.{Map, SeqOps}
import scala.jdk.CollectionConverters._

sealed abstract class InterfaceType extends Product with Serializable {
  def `type`: DefDataType.FWT

  def fold[Z](normal: DefDataType.FWT => Z, template: (Record.FWT, DefTemplate[Type]) => Z): Z =
    this match {
      case InterfaceType.Normal(typ) => normal(typ)
      case InterfaceType.Template(typ, tpl) => template(typ, tpl)
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
}
object InterfaceType {
  final case class Normal(`type`: DefDataType.FWT) extends InterfaceType
  final case class Template(rec: Record.FWT, template: DefTemplate[Type]) extends InterfaceType {
    def `type`: DefDataType.FWT = DefDataType(ImmArraySeq.empty, rec)
  }
}

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
final case class Interface(
    packageId: PackageId,
    metadata: Option[PackageMetadata],
    typeDecls: Map[QualifiedName, InterfaceType],
    astInterfaces: Map[QualifiedName, DefInterface.FWT],
) {
  def getTypeDecls: j.Map[QualifiedName, InterfaceType] = typeDecls.asJava
  def getAstInterfaces: j.Map[QualifiedName, DefInterface.FWT] = astInterfaces.asJava

  private def resolveChoices(
      findInterface: PartialFunction[Ref.TypeConName, DefInterface.FWT],
      failIfUnresolvedChoicesLeft: Boolean,
  ): Interface = {
    val outside = findInterface.lift
    def findIface(id: Identifier) =
      if (id.packageId == packageId) astInterfaces get id.qualifiedName
      else outside(id)
    val tplFindIface = Function unlift findIface
    def transformTemplate(ift: InterfaceType.Template) = {
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
        case ift: InterfaceType.Template => transformTemplate(ift)
        case n: InterfaceType.Normal => n
      }
    })
  }

  /** Like [[EnvironmentInterface#resolveChoices]], but permits incremental
    * resolution of newly-loaded interfaces, such as json-api does.
    *
    * {{{
    *  // suppose
    *  i: Interface; ei: EnvironmentInterface
    *  val eidelta = EnvironmentInterface.fromReaderInterfaces(i)
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
  ): Interface = resolveChoices(findInterface, failIfUnresolvedChoicesLeft = true)

  /** Like resolveChoicesAndFailOnUnresolvableChoices, but simply discard
    * unresolved choices from the structure.  Not wise to use on a receiver
    * without a complete environment provided as the argument.
    */
  def resolveChoicesAndIgnoreUnresolvedChoices(
      findInterface: PartialFunction[Ref.TypeConName, DefInterface.FWT]
  ): Interface = resolveChoices(findInterface, failIfUnresolvedChoicesLeft = false)

  /** Update internal templates, as well as external templates via `setTemplates`,
    * with retroactive interface implementations.
    *
    * @param setTemplate Used to look up templates that can't be found in this
    *                    interface
    */
  private def resolveRetroImplements[S](
      s: S
  )(setTemplate: SetterAt[Ref.TypeConName, S, DefTemplate.FWT]): (S, Interface) = {
    type SandTpls = (S, Map[QualifiedName, InterfaceType.Template])
    def setTpl(
        sm: SandTpls,
        tcn: Ref.TypeConName,
    ): Option[(DefTemplate.FWT => DefTemplate.FWT) => SandTpls] = {
      import Interface.findTemplate
      val (s, tplsM) = sm
      if (tcn.packageId == packageId)
        findTemplate(tplsM, tcn.qualifiedName).map { case itt @ InterfaceType.Template(_, dt) =>
          f => (s, tplsM.updated(tcn.qualifiedName, itt.copy(template = f(dt))))
        }
      else setTemplate(s, tcn) map (_ andThen ((_, tplsM)))
    }

    val ((sEnd, newTpls), newIfcs) = astInterfaces.foldLeft(
      ((s, Map.empty): SandTpls, Map.empty[QualifiedName, DefInterface.FWT])
    ) { case ((s, astIfs), (ifcName, astIf)) =>
      astIf
        .resolveRetroImplements(Ref.TypeConName(packageId, ifcName), s)(setTpl)
        .rightMap(newIf => astIfs.updated(ifcName, newIf))
    }
    (sEnd, copy(typeDecls = typeDecls ++ newTpls, astInterfaces = newIfcs))
  }
}

object Interface {
  import Errors._
  import reader.InterfaceReader._

  def read(lf: DamlLf.Archive): (Errors[ErrorLoc, InvalidDataTypeDefinition], Interface) =
    readInterface(lf)

  def read(lf: ArchivePayload): (Errors[ErrorLoc, InvalidDataTypeDefinition], Interface) =
    readInterface(lf)

  private[iface] def findTemplate[K](
      m: Map[K, InterfaceType],
      k: K,
  ): Option[InterfaceType.Template] =
    m get k collect { case itt: InterfaceType.Template => itt }

  // Given a lookup function for package state setters, produce a lookup function
  // for setters on specific templates in that set of packages.
  private[this] def setPackageTemplates[S](
      findPackage: GetterSetterAt[PackageId, S, Interface]
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
    * `newInterfaces`.  Produce the resulting `S` and a replacement copy of
    * `newInterfaces` with templates and interfaces therein resolved.
    *
    * Does not search members of `s` for fresh interfaces.
    */
  def resolveRetroImplements[S, CC[B] <: Seq[B] with SeqOps[B, CC, CC[B]]](
      s: S,
      newInterfaces: CC[Interface],
  )(
      findPackage: GetterSetterAt[PackageId, S, Interface]
  ): (S, CC[Interface]) = {
    type St = (S, CC[Interface])
    val findTpl = setPackageTemplates[St] { case ((s, newInterfaces), pkgId) =>
      findPackage(s, pkgId).map(_.rightMap(_ andThen ((_, newInterfaces)))).orElse {
        val ix = newInterfaces indexWhere (_.packageId == pkgId)
        (ix >= 0) option ((newInterfaces(ix), newSig => (s, newInterfaces.updated(ix, newSig))))
      }
    }

    (0 until newInterfaces.size).foldLeft((s, newInterfaces)) {
      case (st @ (_, newInterfaces), ifcK) =>
        val ((s2, newInterfaces2), newAtIfcK) =
          newInterfaces(ifcK).resolveRetroImplements(st)(findTpl)
        // the tricky part here: newInterfaces2 is guaranteed not to have altered
        // the value at ifcK, and to have made all "self" changes in newAtIfcK.
        // So there is no conflict, we can discard the value in the seq
        (s2, newInterfaces2.updated(ifcK, newAtIfcK))
    }
  }

  /** An argument for `Interface#resolveChoices` given a package database,
    * such as json-api's `LedgerReader.PackageStore`.
    */
  def findAstInterface(
      findPackage: PartialFunction[PackageId, Interface]
  ): PartialFunction[Ref.TypeConName, DefInterface.FWT] = {
    val pkg = findPackage.lift
    def go(id: Identifier) = pkg(id.packageId).flatMap(_.astInterfaces get id.qualifiedName)
    Function unlift go
  }

}
