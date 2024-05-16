// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts.domain

import com.daml.ledger.api.{v1 => lav1}
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref
import com.daml.nonempty.NonEmpty
import scalaz.{-\/, Applicative, Traverse, \/, \/-}
import scalaz.syntax.functor._

import scala.collection.IterableOps

/** A contract type ID that may be either a template or an interface ID.
  * A [[ContractTypeId.Resolved]] ID will always be either [[ContractTypeId.Template]]
  * or [[ContractTypeId.Interface]]; an
  * unresolved ID may be one of those, which indicates an expectation of what
  * the resolved ID will be, or neither, which indicates that resolving what
  * kind of ID this is will be part of the resolution.
  *
  * Built-in equality is solely determined by the triple of package ID, module
  * name, entity name.  This is because there are likely insidious expectations
  * that this be true dating to before contract type IDs were distinguished at
  * all, and we are only interested in distinguishing them statically, which
  * these types do, and by pattern-matching, which does work.
  *
  * {{{
  *   val selector: ContractTypeId[Unit] = Template((), "M", "E")
  *   selector match {
  *     case ContractTypeId.Unknown(p, m, e) => // this will not match
  *     case ContractTypeId.Interface(p, m, e) => // this will not match
  *     case ContractTypeId.Template(p, m, e) => // this will match
  *   }
  * }}}
  */
sealed abstract class ContractTypeId[+PkgId]
    extends Product3[PkgId, String, String]
    with Serializable
    with ContractTypeId.Ops[ContractTypeId, PkgId] {
  val packageId: PkgId
  val moduleName: String
  val entityName: String
  override def _1 = packageId
  override def _2 = moduleName
  override def _3 = entityName

  // the only way we want to tell the difference dynamically is when
  // pattern-matching.  If we didn't need that for query, we wouldn't even
  // bother with different classes, we would just use different newtypes.
  // Which would yield exactly the following dynamic equality behavior.
  override final def equals(o: Any) = o match {
    case o: ContractTypeId[_] =>
      (this eq o) || {
        packageId == o.packageId && moduleName == o.moduleName && entityName == o.entityName
      }
    case _ => false
  }

  override final def hashCode = {
    import scala.util.hashing.{MurmurHash3 => H}
    H.productHash(this, H.productSeed, ignorePrefix = true)
  }

  def fqn: String = s"${packageId.toString}:${moduleName}:${entityName}"
}

object ResolvedQuery {

  def apply[CtId[T] <: ContractTypeId[T]](resolved: ContractTypeRef[CtId]): ResolvedQuery =
    resolved match {
      case t: ContractTypeRef.TemplateRef => ByTemplateId(t)
      case i: ContractTypeRef.InterfaceRef => ByInterfaceId(i)
    }

  def apply(
      resolved: Set[_ <: ContractTypeRef[ContractTypeId]]
  ): Unsupported \/ ResolvedQuery = {
    import com.daml.nonempty.{NonEmpty, Singleton}
    val (templateIds, interfaceIds) = partitionKPN(resolved)
    templateIds match {
      case NonEmpty(templateIds) =>
        interfaceIds match {
          case NonEmpty(_) => -\/(CannotQueryBothTemplateIdsAndInterfaceIds)
          case _ => \/-(ByTemplateIds(templateIds))
        }
      case _ =>
        interfaceIds match {
          case NonEmpty(Singleton(interfaceId)) => \/-(ByInterfaceId(interfaceId))
          case NonEmpty(_) => -\/(CannotQueryManyInterfaceIds)
          case _ => -\/(CannotBeEmpty)
        }
    }
  }

  def partition[CC[_], C, Pkg](
      resolved: IterableOps[ContractTypeId.Definite[Pkg], CC, C]
  ): (CC[ContractTypeId.Template[Pkg]], CC[ContractTypeId.Interface[Pkg]]) =
    resolved.partitionMap {
      case t @ ContractTypeId.Template(_, _, _) => Left(t)
      case i @ ContractTypeId.Interface(_, _, _) => Right(i)
    }

  private def partitionKPN[CC[_], C](
      resolved: IterableOps[ContractTypeRef[ContractTypeId], CC, C]
  ): (
      CC[ContractTypeRef.TemplateRef],
      CC[ContractTypeRef.InterfaceRef],
  ) =
    resolved.partitionMap {
      case t: ContractTypeRef.TemplateRef => Left(t)
      case i: ContractTypeRef.InterfaceRef => Right(i)
    }

  sealed abstract class Unsupported(val errorMsg: String) extends Product with Serializable
  final case object CannotQueryBothTemplateIdsAndInterfaceIds
      extends Unsupported("Cannot query both templates IDs and interface IDs")
  final case object CannotQueryManyInterfaceIds
      extends Unsupported("Cannot query more than one interface ID")
  final case object CannotBeEmpty extends Unsupported("Cannot resolve any template ID from request")

  final case class ByTemplateIds(
      templateIds: NonEmpty[Set[ContractTypeRef.TemplateRef]]
  ) extends ResolvedQuery {
    def resolved = templateIds
  }
  final case class ByTemplateId(
      templateId: ContractTypeRef.TemplateRef
  ) extends ResolvedQuery {
    def resolved = NonEmpty(Set, templateId)
  }

  final case class ByInterfaceId(
      interfaceId: ContractTypeRef.InterfaceRef
  ) extends ResolvedQuery {
    def resolved = NonEmpty(Set, interfaceId)
  }
}

sealed abstract class ResolvedQuery extends Product with Serializable {
  def resolved: NonEmpty[Set[_ <: ContractTypeRef.Resolved]]
}

object ContractTypeId extends ContractTypeIdLike[ContractTypeId] {
  final case class Unknown[+PkgId](
      packageId: PkgId,
      moduleName: String,
      entityName: String,
  ) extends ContractTypeId[PkgId]
      with Ops[Unknown, PkgId] {
    override def productPrefix = "ContractTypeId"

    override def copy[PkgId0](
        packageId: PkgId0 = packageId,
        moduleName: String = moduleName,
        entityName: String = entityName,
    ) = Unknown(packageId, moduleName, entityName)
  }

  sealed abstract class Definite[+PkgId] extends ContractTypeId[PkgId] with Ops[Definite, PkgId]

  /** A contract type ID known to be a template, not an interface.  When resolved,
    * it indicates that the LF environment associates this ID with a template.
    * When unresolved, it indicates that the intent is to search only template
    * IDs for resolution, and that resolving to an interface ID should be an error.
    */
  final case class Template[+PkgId](packageId: PkgId, moduleName: String, entityName: String)
      extends Definite[PkgId]
      with Ops[Template, PkgId] {
    override def productPrefix = "TemplateId"

    override def copy[PkgId0](
        packageId: PkgId0 = packageId,
        moduleName: String = moduleName,
        entityName: String = entityName,
    ) = Template(packageId, moduleName, entityName)
  }

  /** A contract type ID known to be an interface, not a template.  When resolved,
    * it indicates that the LF environment associates this ID with an interface.
    * When unresolved, it indicates that the intent is to search only interface
    * IDs for resolution, and that resolving to a template ID should be an error.
    */
  final case class Interface[+PkgId](packageId: PkgId, moduleName: String, entityName: String)
      extends Definite[PkgId]
      with Ops[Interface, PkgId] {
    override def productPrefix = "InterfaceId"

    override def copy[PkgId0](
        packageId: PkgId0 = packageId,
        moduleName: String = moduleName,
        entityName: String = entityName,
    ) = Interface(packageId, moduleName, entityName)
  }

  override def apply[PkgId](
      packageId: PkgId,
      moduleName: String,
      entityName: String,
  ): ContractTypeId[PkgId] =
    Unknown(packageId, moduleName, entityName)

  // Product3 makes custom unapply really cheap
  def unapply[PkgId](ctId: ContractTypeId[PkgId]): Some[ContractTypeId[PkgId]] = Some(ctId)

  // belongs in ultimate parent `object`
  implicit def `ContractTypeId covariant`[F[T] <: ContractTypeId[T] with ContractTypeId.Ops[F, T]]
      : Traverse[F] =
    new Traverse[F] {
      override def map[A, B](fa: F[A])(f: A => B): F[B] =
        fa.copy(packageId = f(fa.packageId))

      override def traverseImpl[G[_]: Applicative, A, B](fa: F[A])(f: A => G[B]): G[F[B]] =
        f(fa.packageId) map (p2 => fa.copy(packageId = p2))
    }

  object Unknown extends Like[Unknown]

  object Template extends Like[Template]

  object Interface extends Like[Interface]

  // TODO #14727 make an opaque subtype, produced by PackageService on
  // confirmed-present IDs only.  Can probably start by adding
  // `with Definite[Any]` here and seeing what happens
  /** A resolved [[ContractTypeId]], typed `CtTyId`. */

  type WithDefiniteOps[+CtId[_], Pkg] = CtId[Pkg] with Definite[Pkg] with Ops[CtId, Pkg]

  type ResolvedOf[+CtId[_]] = WithDefiniteOps[CtId, Ref.PackageRef]
  type ResolvedPkgIdOf[+CtId[_]] = WithDefiniteOps[CtId, Ref.PackageId]

  type Like[CtId[T] <: ContractTypeId[T]] = ContractTypeIdLike[CtId]

  // CtId serves the same role as `CC` on scala.collection.IterableOps
  sealed trait Ops[+CtId[_], +PkgId] { this: ContractTypeId[PkgId] =>
    def copy[PkgId0](
        packageId: PkgId0 = packageId,
        moduleName: String = moduleName,
        entityName: String = entityName,
    ): CtId[PkgId0] with Ops[CtId, PkgId0]
  }

  def withPkgRef[CtId[T] <: ContractTypeId[T]](
      id: CtId[Ref.PackageId] with Ops[CtId, Ref.PackageId]
  ): CtId[Ref.PackageRef] =
    id.copy(packageId = Ref.PackageRef.Id(id.packageId): Ref.PackageRef)
}

/** A contract type ID companion. */
sealed abstract class ContractTypeIdLike[CtId[T] <: ContractTypeId[T]] {
  type RequiredPkg = CtId[Ref.PackageRef]
  type RequiredPkgId = CtId[Ref.PackageId]
  type Resolved = ContractTypeId.ResolvedOf[CtId]
  type ResolvedPkgId = ContractTypeId.ResolvedPkgIdOf[CtId]

  // treat the companion like a typeclass instance
  implicit def `ContractTypeIdLike companion`: this.type = this

  def apply[PkgId](
      packageId: PkgId,
      moduleName: String,
      entityName: String,
  ): CtId[PkgId] with ContractTypeId.Ops[CtId, PkgId]

  final def fromLedgerApi(in: lav1.value.Identifier): RequiredPkgId =
    apply(Ref.PackageId.assertFromString(in.packageId), in.moduleName, in.entityName)

  private[this] def qualifiedName(a: CtId[_]): Ref.QualifiedName =
    Ref.QualifiedName(
      Ref.DottedName.assertFromString(a.moduleName),
      Ref.DottedName.assertFromString(a.entityName),
    )

  final def toLedgerApiValue(a: RequiredPkgId): Ref.Identifier = {
    val qfName = qualifiedName(a)
    Ref.Identifier(a.packageId, qfName)
  }
}

// Represents information about a contract type id that may use a name as the package reference.
// i.e. `orig` may have a form or either "#foo:Bar:Baz" or "123:Bar:Baz".
// If a package name is provided, then `allIds` may resolve to multiple values, for the relevant
// template in each package id that shares the same package name.
sealed abstract class ContractTypeRef[+CtTyId[T] <: ContractTypeId[T]](
    orig: ContractTypeId.ResolvedOf[CtTyId],
    ids: NonEmpty[Seq[ContractTypeId.ResolvedPkgIdOf[CtTyId]]],
    val name: KeyPackageName,
) {
  def allPkgIds: NonEmpty[Set[_ <: ContractTypeId.ResolvedPkgIdOf[CtTyId]]] = ids.toSet
  def latestPkgId: ContractTypeId.ResolvedPkgIdOf[CtTyId] = ids.head
  def original: ContractTypeId.ResolvedOf[CtTyId] = orig
}

object ContractTypeRef {
  type Resolved = ContractTypeRef[ContractTypeId]

  def unnamed[CtTyId[T] <: ContractTypeId[T]](
      id: ContractTypeId.ResolvedPkgIdOf[CtTyId]
  ): ContractTypeRef[CtTyId] = {
    val idWithRef = id.copy(packageId = Ref.PackageRef.Id(id.packageId): Ref.PackageRef)
    apply[CtTyId](idWithRef, NonEmpty(Seq, id.packageId), KeyPackageName.empty)
  }

  def apply[CtId[T] <: ContractTypeId[T]](
      id: ContractTypeId.ResolvedOf[CtId], // The original template/interface id.
      pkgIds: NonEmpty[Seq[Ref.PackageId]], // Package ids with same name, by version, descending
      name: KeyPackageName, // The package name info
  ): ContractTypeRef[CtId] = {
    ((id: ContractTypeId.Definite[Ref.PackageRef]) match {
      case t @ ContractTypeId.Template(_, _, _) => TemplateRef(t, pkgIds, name)
      case i @ ContractTypeId.Interface(_, _, _) => InterfaceRef(i, pkgIds, name)
    }).asInstanceOf[ContractTypeRef[CtId]] // CtId <: Definite, because id is resolved
  }

  private[domain] final case class TemplateRef(
      orig: ContractTypeId.Template.Resolved,
      pkgIds: NonEmpty[Seq[Ref.PackageId]],
      kpn: KeyPackageName,
  ) extends ContractTypeRef[ContractTypeId.Template](
        orig,
        pkgIds.map(pkgId => orig.copy(packageId = pkgId)),
        kpn,
      )

  private[domain] final case class InterfaceRef(
      orig: ContractTypeId.Interface.Resolved,
      pkgIds: NonEmpty[Seq[Ref.PackageId]],
      kpn: KeyPackageName,
  ) extends ContractTypeRef[ContractTypeId.Interface](
        orig,
        pkgIds.map(pkgId => orig.copy(packageId = pkgId)),
        kpn,
      )
}
