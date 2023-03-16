// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref._
import com.daml.lf.data.{BackStack, FrontStack, ImmArray}
import com.daml.lf.language.Ast._
import com.daml.lf.transaction.GlobalKeyWithMaintainers
import com.daml.lf.value.Value._
import scalaz.Monad

import scala.annotation.tailrec

/** many operations require to look up packages and contracts. we do this
  * by allowing our functions to pause and resume after the contract has been
  * fetched.
  */
sealed trait Result[+A] extends Product with Serializable {
  def map[B](f: A => B): Result[B] = this match {
    case ResultInterruption(continue) => ResultInterruption(() => continue().map(f))
    case ResultDone(x) => ResultDone(f(x))
    case ResultError(err) => ResultError(err)
    case ResultNeedContract(acoid, resume) =>
      ResultNeedContract(acoid, mbContract => resume(mbContract).map(f))
    case ResultNeedPackage(pkgId, resume) =>
      ResultNeedPackage(pkgId, mbPkg => resume(mbPkg).map(f))
    case ResultNeedKey(gk, resume) =>
      ResultNeedKey(gk, mbAcoid => resume(mbAcoid).map(f))
    case ResultNeedAuthority(holding, requesting, resume) =>
      ResultNeedAuthority(holding, requesting, bool => resume(bool).map(f))
  }

  def flatMap[B](f: A => Result[B]): Result[B] = this match {
    case ResultInterruption(continue) => ResultInterruption(() => continue().flatMap(f))
    case ResultDone(x) => f(x)
    case ResultError(err) => ResultError(err)
    case ResultNeedContract(acoid, resume) =>
      ResultNeedContract(acoid, mbContract => resume(mbContract).flatMap(f))
    case ResultNeedPackage(pkgId, resume) =>
      ResultNeedPackage(pkgId, mbPkg => resume(mbPkg).flatMap(f))
    case ResultNeedKey(gk, resume) =>
      ResultNeedKey(gk, mbAcoid => resume(mbAcoid).flatMap(f))
    case ResultNeedAuthority(holding, requesting, resume) =>
      ResultNeedAuthority(holding, requesting, bool => resume(bool).flatMap(f))
  }

  private[lf] def consume(
      pcs: ContractId => Option[VersionedContractInstance],
      packages: PackageId => Option[Package],
      keys: GlobalKeyWithMaintainers => Option[ContractId],
      grantNeedAuthority: Boolean = false,
  ): Either[Error, A] = {
    @tailrec
    def go(res: Result[A]): Either[Error, A] =
      res match {
        case ResultDone(x) => Right(x)
        case ResultInterruption(continue) => go(continue())
        case ResultError(err) => Left(err)
        case ResultNeedContract(acoid, resume) => go(resume(pcs(acoid)))
        case ResultNeedPackage(pkgId, resume) => go(resume(packages(pkgId)))
        case ResultNeedKey(key, resume) => go(resume(keys(key)))
        case ResultNeedAuthority(_, _, resume) => go(resume(grantNeedAuthority))
      }
    go(this)
  }
}

final case class ResultInterruption[A](continue: () => Result[A]) extends Result[A]

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

/** Intermediate result indicating that a [[ContractInstance]] is required to complete the computation.
  * To resume the computation, the caller must invoke `resume` with the following argument:
  * <ul>
  * <li>`Some(contractInstance)`, if the caller can dereference `acoid` to `contractInstance`</li>
  * <li>`None`, if the caller is unable to dereference `acoid`</li>
  * </ul>
  *
  * The caller of `resume` has to ensure that the contract instance passed to `resume` is a contract instance that
  * has previously been associated with `acoid` by the engine.
  * The engine does not validate the given contract instance.
  */
final case class ResultNeedContract[A](
    acoid: ContractId,
    resume: Option[VersionedContractInstance] => Result[A],
) extends Result[A]

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
  *
  * TODO: may `contractId` refer to an archived contract?
  * TODO: may `resume` be called with `None` even when the key is assigned?
  */
final case class ResultNeedKey[A](
    key: GlobalKeyWithMaintainers,
    resume: Option[ContractId] => Result[A],
) extends Result[A]

/** TODO: I think this deserves a comment, but don't know what to write.
  */
final case class ResultNeedAuthority[A](
    holding: Set[Party],
    requesting: Set[Party],
    resume: Boolean => Result[A],
) extends Result[A]

object Result {
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
      resume: VersionedContractInstance => Result[A],
  ) =
    ResultNeedContract(
      acoid,
      {
        case None =>
          ResultError(
            Error.Interpretation.DamlException(interpretation.Error.ContractNotFound(acoid))
          )
        case Some(contract) => resume(contract)
      },
    )

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
            case ResultNeedAuthority(holding, requesting, resume) =>
              ResultNeedAuthority(
                holding,
                requesting,
                bool =>
                  resume(bool).flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
            case ResultNeedContract(acoid, resume) =>
              ResultNeedContract(
                acoid,
                coinst =>
                  resume(coinst).flatMap(x =>
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
            case ResultInterruption(continue) =>
              ResultInterruption(() =>
                continue().flatMap(x =>
                  Result
                    .sequence(results_)
                    .map(otherResults => (okResults :+ x) :++ otherResults)
                )
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

  implicit val resultInstance: Monad[Result] = new Monad[Result] {
    override def point[A](a: => A): Result[A] = ResultDone(a)
    override def bind[A, B](fa: Result[A])(f: A => Result[B]): Result[B] = fa.flatMap(f)
  }
}
