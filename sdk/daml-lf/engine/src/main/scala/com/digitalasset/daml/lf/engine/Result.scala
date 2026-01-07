// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import cats.Applicative
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{BackStack, FrontStack, ImmArray}
import com.digitalasset.daml.lf.engine.ResultNeedContract.Response
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
}
import com.digitalasset.daml.lf.value.Value._
import scalaz.Monad

import scala.annotation.tailrec

/** many operations require to look up packages and contracts. we do this
  * by allowing our functions to pause and resume after the contract has been
  * fetched.
  */
sealed trait Result[+A] extends Product with Serializable {
  def map[B](f: A => B): Result[B] = this match {
    case ResultInterruption(continue, abort) =>
      ResultInterruption(() => continue().map(f), abort)
    case ResultDone(x) => ResultDone(f(x))
    case ResultError(err) => ResultError(err)
    case ResultNeedContract(coid, resume) =>
      ResultNeedContract(coid, response => resume(response).map(f))
    case ResultNeedPackage(pkgId, resume) =>
      ResultNeedPackage(pkgId, mbPkg => resume(mbPkg).map(f))
    case ResultNeedKey(gk, resume) =>
      ResultNeedKey(gk, mbAcoid => resume(mbAcoid).map(f))
    case ResultPrefetch(contractIds, keys, resume) =>
      ResultPrefetch(contractIds, keys, () => resume().map(f))
  }

  def flatMap[B](f: A => Result[B]): Result[B] = this match {
    case ResultInterruption(continue, abort) =>
      ResultInterruption(() => continue().flatMap(f), abort)
    case ResultDone(x) => f(x)
    case ResultError(err) => ResultError(err)
    case ResultNeedContract(coid, resume) =>
      ResultNeedContract(coid, response => resume(response).flatMap(f))
    case ResultNeedPackage(pkgId, resume) =>
      ResultNeedPackage(pkgId, mbPkg => resume(mbPkg).flatMap(f))
    case ResultNeedKey(gk, resume) =>
      ResultNeedKey(gk, mbAcoid => resume(mbAcoid).flatMap(f))
    case ResultPrefetch(contractIds, keys, resume) =>
      ResultPrefetch(contractIds, keys, () => resume().flatMap(f))
  }

  private[lf] def consume(
      pcs: PartialFunction[ContractId, FatContractInstance] = PartialFunction.empty,
      pkgs: PartialFunction[PackageId, Package] = PartialFunction.empty,
      keys: PartialFunction[GlobalKeyWithMaintainers, ContractId] = PartialFunction.empty,
      hashingMethod: ContractId => Hash.HashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
      idValidator: (ContractId, Hash) => Boolean = (_, _) => true,
  ): Either[Error, A] = {
    @tailrec
    def go(res: Result[A]): Either[Error, A] =
      res match {
        case ResultDone(x) => Right(x)
        case ResultInterruption(continue, _) => go(continue())
        case ResultError(err) => Left(err)
        case ResultNeedContract(acoid, resume) =>
          go(resume(pcs.lift(acoid) match {
            case None => ResultNeedContract.Response.ContractNotFound
            case Some(coInst) =>
              Response.ContractFound(coInst, hashingMethod(acoid), idValidator(acoid, _))
          }))
        case ResultNeedPackage(pkgId, resume) => go(resume(pkgs.lift(pkgId)))
        case ResultNeedKey(key, resume) => go(resume(keys.lift(key)))
        case ResultPrefetch(_, _, result) => go(result())
      }
    go(this)
  }
}

final case class ResultInterruption[A](continue: () => Result[A], abort: () => Option[String])
    extends Result[A]

/** Indicates that the command (re)interpretation was successful.
  */
final case class ResultDone[A](result: A) extends Result[A]
object ResultDone {
  val Unit: ResultDone[Unit] = new ResultDone(())
}

/** Indicates that the command (re)interpretation has failed.
  */
final case class ResultError(err: Error) extends Result[Nothing]
object ResultError {
  def apply(packageError: Error.Package.Error): ResultError =
    ResultError(Error.Package(packageError))
  def apply(preprocessingError: Error.Preprocessing.Error): ResultError =
    ResultError(Error.Preprocessing(preprocessingError))
  def apply(
      interpretationError: Error.Interpretation.Error,
      details: Option[String] = None,
  ): ResultError =
    ResultError(Error.Interpretation(interpretationError, details))
  def apply(validationError: Error.Validation.Error): ResultError =
    ResultError(Error.Validation(validationError))
}

/** Intermediate result indicating that a [[ResultNeedContract.Response]] is required to complete the computation.
  * To resume the computation, the caller must invoke `resume` with the following argument:
  *   - `ContractFound(...)`, if the caller can dereference `coid` to a fat contract instance in the contract store or
  *     in the command's explicit disclosures.
  *   - `NotFound`, if the caller is unable to dereference `coid`
  *   - `UnsupportedContractIdVersion` if the caller is able to dereference `coid` but the resulting contract's id is
  *     malformed or uses an unsupported version.
  *
  * In the `ContractFound` case, the caller of `resume` must provide the following information:
  *   - `contractInstance`: The fat contract instance that has previously been associated with `coid` by the engine. The
  *      caller must not / cannot authenticate or validate the FatContractInstance aside from extracting the hashing
  *      scheme version from its contract ID.
  *   - `expectedHashingMethod`: The hashing method that the engine expects the engine to use for authenticating the
  *      contract instance.
  *   - `idValidator`: A function that authenticates the contract given a hash of the contract instance computed by the
  *      engine using the `expectedHashingMethod`.
  */
final case class ResultNeedContract[A](
    coid: ContractId,
    resume: ResultNeedContract.Response => Result[A],
) extends Result[A]

object ResultNeedContract {
  sealed trait Response

  object Response {
    final case class ContractFound(
        contractInstance: FatContractInstance,
        expectedHashingMethod: Hash.HashingMethod,
        idValidator: Hash => Boolean,
    ) extends Response

    final case object ContractNotFound extends Response

    final case object UnsupportedContractIdVersion extends Response
  }
}

/** Intermediate result indicating that a [[Package]] is required to complete the computation.
  * To resume the computation, the caller must invoke `resume` with the following argument:
  * <ul>
  * <li>`Some(package)`, if the caller can dereference `packageId` to `package`</li>
  * <li>`None`, if the caller is unable to dereference `packageId`</li>
  * </ul>
  *
  * It depends on the engine configuration whether the engine will validate the package provided to `resume`.
  * If validation is switched off, it is the callers responsibility to provide a valid package corresponding to `packageId`.
  */
final case class ResultNeedPackage[A](packageId: PackageId, resume: Option[Package] => Result[A])
    extends Result[A]

/** Intermediate result indicating that the contract id corresponding to a key is required to complete the computation.
  * To resume the computation, the caller must invoke `resume` with the following argument:
  * <ul>
  * <li>`Some(contractId)`, if `key` is currently assigned to `contractId`</li>
  * <li>`None`, if `key` is unassigned</li>
  * </ul>
  *
  * The caller of `resume` has to ensure that any contract id passed to `resume` has previously been associated with
  * a contract with `key` as a key.
  * Other than that, the caller does not need to validate the data passed to `resume`. In particular, it may pass
  * the id of an archived contract to `resume`.
  * It may also provide `None` to `resume` when the `key` is actually assigned.
  */
final case class ResultNeedKey[A](
    key: GlobalKeyWithMaintainers,
    resume: Option[ContractId] => Result[A],
) extends Result[A]

/** Indicates that the interpretation will likely need to resolve the given contract keys.
  * The caller may resolve the keys in parallel to the interpretation, but does not have to.
  */
final case class ResultPrefetch[A](
    contractIds: Seq[ContractId],
    keys: Seq[GlobalKey],
    resume: () => Result[A],
) extends Result[A]

object Result {

  val unit: ResultDone[Unit] = ResultDone(())

  // fails with ResultError if the package is not found
  private[lf] def needPackage[A](
      packageId: PackageId,
      context: language.Reference,
      resume: Package => Result[A],
  ) =
    ResultNeedPackage(
      packageId,
      {
        case Some(pkg) => resume(pkg)
        case None => ResultError(Error.Package.MissingPackage(packageId, context))
      },
    )

  private[lf] def needContract[A](
      acoid: ContractId,
      resume: (FatContractInstance, Hash.HashingMethod, Hash => Boolean) => Result[A],
  ) = {
    import ResultNeedContract.Response._
    ResultNeedContract(
      acoid,
      {
        case ContractFound(contractInstance, expectedHashingMethod, authenticator) =>
          resume(contractInstance, expectedHashingMethod, authenticator)
        case ContractNotFound =>
          ResultError(
            Error.Interpretation.DamlException(interpretation.Error.ContractNotFound(acoid))
          )
        case UnsupportedContractIdVersion =>
          throw new NotImplementedError(
            "UnsupportedContractIdVersion is not yet supported."
          )
      },
    )
  }

  def sequence[A](results0: FrontStack[Result[A]]): Result[ImmArray[A]] = {
    @tailrec
    def go(okResults: BackStack[A], results: FrontStack[Result[A]]): Result[BackStack[A]] =
      results.pop match {
        case None => ResultDone(okResults)
        case Some((res, results_)) =>
          res match {
            case ResultDone(x) => go(okResults :+ x, results_)
            case ResultError(err) => ResultError(err)
            case ResultNeedPackage(packageId, resume) =>
              ResultNeedPackage(
                packageId,
                pkg =>
                  resume(pkg).flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
            case ResultNeedContract(coid, resume) =>
              ResultNeedContract(
                coid,
                response =>
                  resume(response).flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
            case ResultNeedKey(gk, resume) =>
              ResultNeedKey(
                gk,
                mbAcoid =>
                  resume(mbAcoid).flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
            case ResultInterruption(continue, abort) =>
              ResultInterruption(
                () =>
                  continue().flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
                abort,
              )
            case ResultPrefetch(contractIds, keys, resume) =>
              ResultPrefetch(
                contractIds,
                keys,
                () =>
                  resume().flatMap(x =>
                    Result.sequence(results_).map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
          }
      }
    go(BackStack.empty, results0).map(_.toImmArray)
  }

  def assert(assertion: Boolean)(err: Error): Result[Unit] =
    if (assertion)
      ResultDone.Unit
    else
      ResultError(err)

  object ResultInstances {

    implicit val resultMonadInstance: Monad[Result] = new Monad[Result] {
      override def point[A](a: => A): Result[A] = ResultDone(a)

      override def bind[A, B](fa: Result[A])(f: A => Result[B]): Result[B] = fa.flatMap(f)
    }

    implicit val resultApplicativeInstance: Applicative[Result] = new Applicative[Result] {
      override def pure[A](x: A): Result[A] = ResultDone(x)

      override def ap[A, B](ff: Result[A => B])(fa: Result[A]): Result[B] =
        fa.flatMap(a => ff.map(f => f(a)))
    }
  }
}
