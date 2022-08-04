package com.daml.fetchcontracts.domain

import com.daml.ledger.api.{v1 => lav1}
import com.daml.lf.data.Ref
import scalaz.{Traverse, Applicative}
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

object ContractTypeId extends ContractTypeIdLike[ContractTypeId] {
  // TODO SC rename to Unknown
  final case class UnknownImpl[+PkgId](
      packageId: PkgId,
      moduleName: String,
      entityName: String,
  ) extends ContractTypeId[PkgId]
      with Ops[UnknownImpl, PkgId] {
    override def productPrefix = "ContractTypeId"

    override def copy[PkgId0](
        packageId: PkgId0 = packageId,
        moduleName: String = moduleName,
        entityName: String = entityName,
    ) = UnknownImpl(packageId, moduleName, entityName)
  }

  // TODO SC placeholder for the lub of Template/Interface
  type Definite[+PkgId] = ContractTypeId[PkgId]

  /** A contract type ID known to be a template, not an interface.  When resolved,
    * it indicates that the LF environment associates this ID with a template.
    * When unresolved, it indicates that the intent is to search only template
    * IDs for resolution, and that resolving to an interface ID should be an error.
    */
  type Template[+PkgId] = ContractTypeId[PkgId]

  /** A contract type ID known to be an interface, not a template.  When resolved,
    * it indicates that the LF environment associates this ID with an interface.
    * When unresolved, it indicates that the intent is to search only interface
    * IDs for resolution, and that resolving to a template ID should be an error.
    */
  final case class Interface[+PkgId](packageId: PkgId, moduleName: String, entityName: String)
      extends ContractTypeId[PkgId]
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
    UnknownImpl(packageId, moduleName, entityName)

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

  val Template = this

  object Interface extends Like[Interface]

  // TODO #14067 make an opaque subtype, produced by PackageService on
  // confirmed-present IDs only
  /** A resolved [[ContractTypeId]], typed `CtTyId`. */
  type ResolvedId[+CtTyId] = CtTyId

  type Like[CtId[T] <: ContractTypeId[T]] = ContractTypeIdLike[CtId]

  sealed trait Ops[+CtId[_], +PkgId] { this: ContractTypeId[PkgId] =>
    def copy[PkgId0](
        packageId: PkgId0 = packageId,
        moduleName: String = moduleName,
        entityName: String = entityName,
    ): CtId[PkgId0]
  }
}

/** A contract type ID companion. */
abstract class ContractTypeIdLike[CtId[T] <: ContractTypeId[T]] {
  type OptionalPkg = CtId[Option[String]]
  type RequiredPkg = CtId[String]
  type NoPkg = CtId[Unit]
  type Resolved = ContractTypeId.ResolvedId[RequiredPkg]

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
