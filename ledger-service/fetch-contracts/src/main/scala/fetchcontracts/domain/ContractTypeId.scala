// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts.domain

import com.daml.ledger.api.{v1 => lav1}
import com.daml.lf.data.Ref
import scalaz.{-\/, Applicative, Traverse, \/, \/-}
import scalaz.syntax.functor._

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
}

object ResolvedQuery {
  def apply(resolved: ContractTypeId.Resolved): ResolvedQuery = {
    resolved match {
      case t: ContractTypeId.Template.Resolved => ByTemplateId(t)
      case i: ContractTypeId.Interface.Resolved => ByInterfaceId(i)
      case unexpected => throw new Exception(s"unexpected,  ContractTypeId.Resolved is $unexpected")
    }
  }
  def apply(resolved: Set[ContractTypeId.Resolved]): Unsupported \/ ResolvedQuery = {
    val (templateIds, interfaceIds) = resolved.partitionMap {
      // TODO 'Resolved' only is non-exhaustive, which should not be the case.
      case t: ContractTypeId.Template.Resolved =>
        Left[ContractTypeId.Template.Resolved, ContractTypeId.Interface.Resolved](t)
      case i: ContractTypeId.Interface.Resolved =>
        Right[ContractTypeId.Template.Resolved, ContractTypeId.Interface.Resolved](i)
      case unexpected => throw new Exception(s"unexpected,  ContractTypeId.Resolved is $unexpected")
    }
    if (templateIds.nonEmpty && interfaceIds.nonEmpty) {
      -\/(CannotQueryBothTemplateIdsAndInterfaceIds)
    } else if (templateIds.isEmpty && interfaceIds.size != 1) {
      -\/(CannotQueryManyInterfaceIds)
    } else if (templateIds.isEmpty && interfaceIds.size == 1) {
      \/-(ByInterfaceId(interfaceIds.head))
    } else if (templateIds.nonEmpty) {
      \/-(ByTemplateIds(templateIds))
    } else {
      -\/(CannotBeEmpty)
    }
  }
  sealed trait Unsupported
  final case object CannotQueryBothTemplateIdsAndInterfaceIds extends Unsupported
  final case object CannotQueryManyInterfaceIds extends Unsupported
  final case object CannotBeEmpty extends Unsupported

  // TODO RR #14871 verify that `ResolvedQuery.Empty` is ok where it is used
  final case object Empty extends ResolvedQuery {
    def resolved = Set.empty[ContractTypeId.Resolved]
  }

  // TODO SC Should only be used by WebsocketService, assuming the ids are verified earlier upstream.
  final case class ContractTypeIdsQuery(ids: Set[ContractTypeId.Resolved]) extends ResolvedQuery {
    def resolved: Set[ContractTypeId.Resolved] = ids
  }

  final case class ByTemplateIds(templateIds: Set[ContractTypeId.Template.Resolved])
      extends ResolvedQuery {
    def resolved: Set[ContractTypeId.Resolved] =
      templateIds.toSet[ContractTypeId.Resolved]
  }
  final case class ByTemplateId(templateId: ContractTypeId.Template.Resolved)
      extends ResolvedQuery {
    def resolved: Set[ContractTypeId.Resolved] =
      Set(templateId).toSet[ContractTypeId.Resolved]
  }

  final case class ByInterfaceId(interfaceId: ContractTypeId.Interface.Resolved)
      extends ResolvedQuery {
    def resolved: Set[ContractTypeId.Resolved] =
      Set(interfaceId).toSet[ContractTypeId.Resolved]
  }
}

sealed trait ResolvedQuery {
  def resolved: Set[ContractTypeId.Resolved]
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
  type ResolvedId[+CtTyId] = CtTyId

  type ResolvedOf[+CtId[_]] = ResolvedId[CtId[String] with Definite[String]]

  type Like[CtId[T] <: ContractTypeId[T]] = ContractTypeIdLike[CtId]

  // CtId serves the same role as `CC` on scala.collection.IterableOps
  sealed trait Ops[+CtId[_], +PkgId] { this: ContractTypeId[PkgId] =>
    def copy[PkgId0](
        packageId: PkgId0 = packageId,
        moduleName: String = moduleName,
        entityName: String = entityName,
    ): CtId[PkgId0]
  }
}

/** A contract type ID companion. */
sealed abstract class ContractTypeIdLike[CtId[T] <: ContractTypeId[T]] {
  type OptionalPkg = CtId[Option[String]]
  type RequiredPkg = CtId[String]
  type NoPkg = CtId[Unit]
  type Resolved = ContractTypeId.ResolvedOf[CtId]

  // treat the companion like a typeclass instance
  implicit def `ContractTypeIdLike companion`: this.type = this

  def apply[PkgId](packageId: PkgId, moduleName: String, entityName: String): CtId[PkgId]

  final def fromLedgerApi(in: lav1.value.Identifier): RequiredPkg =
    apply(in.packageId, in.moduleName, in.entityName)

  private[this] def qualifiedName(a: CtId[_]): Ref.QualifiedName =
    Ref.QualifiedName(
      Ref.DottedName.assertFromString(a.moduleName),
      Ref.DottedName.assertFromString(a.entityName),
    )

  final def toLedgerApiValue(a: RequiredPkg): Ref.Identifier = {
    val qfName = qualifiedName(a)
    val packageId = Ref.PackageId.assertFromString(a.packageId)
    Ref.Identifier(packageId, qfName)
  }
}
