package com.daml.fetchcontracts.domain

import com.daml.lf.data.Ref
import scalaz.{Traverse, Applicative}
import scalaz.syntax.functor._

// TODO #14067 make these separate types
object ContractTypeId {
  // TODO #14067 Resolved makes the semantics a little tricky; we would like
  // to prove that Template and Interface completely partition the
  // Resolved[Unknown[_]] type, which is not true of the unadorned unresolved
  // contract type IDs
  sealed abstract class Unknown[+PkgId] extends Product3[PkgId, String, String] with Serializable {
    val packageId: PkgId
    val moduleName: String
    val entityName: String
    override def _1 = packageId
    override def _2 = moduleName
    override def _3 = entityName
  }

  /** A contract type ID known to be a template, not an interface. */
  type Template[+PkgId] = Unknown[PkgId] // <: Unknown
  /** A contract type ID known to be an interface, not a template. */
  type Interface[+PkgId] = Unknown[PkgId] // <: Unknown

  object Unknown extends Like[Unknown] {
    private[this] final case class UnknownImpl[+PkgId](
        packageId: PkgId,
        moduleName: String,
        entityName: String,
    ) extends Unknown[PkgId] {
      override def productPrefix = "ContractTypeId"
    }

    override def apply[PkgId](
        packageId: PkgId,
        moduleName: String,
        entityName: String,
    ): Unknown[PkgId] =
      UnknownImpl(packageId, moduleName, entityName)

    // Product3 makes custom unapply really cheap
    def unapply[PkgId](ctId: Unknown[PkgId]): Some[Unknown[PkgId]] = Some(ctId)

    // belongs in ultimate parent `object`
    implicit def `ContractTypeId covariant`[F[T] <: ContractTypeId.Unknown[T]](implicit
        companion: ContractTypeId.Like[F]
    ): Traverse[F] =
      new Traverse[F] {
        override def map[A, B](fa: F[A])(f: A => B): F[B] =
          companion(f(fa.packageId), fa.moduleName, fa.entityName)

        override def traverseImpl[G[_]: Applicative, A, B](fa: F[A])(f: A => G[B]): G[F[B]] =
          f(fa.packageId) map (companion(_, fa.moduleName, fa.entityName))
      }
  }

  val Template = Unknown
  val Interface = Unknown

  // TODO #14067 make an opaque subtype, produced by PackageService on
  // confirmed-present IDs only
  /** A resolved [[ContractTypeId]], typed `CtTyId`. */
  type Resolved[+CtTyId] = CtTyId

  /** A contract type ID companion. */
  abstract class Like[CtId[T] <: Unknown[T]] {
    type OptionalPkg = CtId[Option[String]]
    type RequiredPkg = CtId[String]
    type NoPkg = CtId[Unit]
    type Resolved = ContractTypeId.Resolved[RequiredPkg]

    // treat the companion like a typeclass instance
    implicit def `ContractTypeId.Like companion`: this.type = this

    def apply[PkgId](packageId: PkgId, moduleName: String, entityName: String): CtId[PkgId]

    private[this] def qualifiedName(a: CtId[_]): Ref.QualifiedName =
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      )

    def toLedgerApiValue(a: RequiredPkg): Ref.Identifier = {
      val qfName = qualifiedName(a)
      val packageId = Ref.PackageId.assertFromString(a.packageId)
      Ref.Identifier(packageId, qfName)
    }
  }
}
