// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import com.daml.lf.data.Ref._
import com.daml.lf.data.{BackStack, ImmArray, ImmArrayCons}
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
    case ResultDone(x) => ResultDone(f(x))
    case ResultError(err) => ResultError(err)
    case ResultNeedContract(acoid, resume) =>
      ResultNeedContract(acoid, mbContract => resume(mbContract).map(f))
    case ResultNeedPackage(pkgId, resume) =>
      ResultNeedPackage(pkgId, mbPkg => resume(mbPkg).map(f))
    case ResultNeedKey(gk, resume) =>
      ResultNeedKey(gk, mbAcoid => resume(mbAcoid).map(f))
  }

  def flatMap[B](f: A => Result[B]): Result[B] = this match {
    case ResultDone(x) => f(x)
    case ResultError(err) => ResultError(err)
    case ResultNeedContract(acoid, resume) =>
      ResultNeedContract(acoid, mbContract => resume(mbContract).flatMap(f))
    case ResultNeedPackage(pkgId, resume) =>
      ResultNeedPackage(pkgId, mbPkg => resume(mbPkg).flatMap(f))
    case ResultNeedKey(gk, resume) =>
      ResultNeedKey(gk, mbAcoid => resume(mbAcoid).flatMap(f))
  }

  def consume(
      pcs: ContractId => Option[ContractInst[VersionedValue[ContractId]]],
      packages: PackageId => Option[Package],
      keys: GlobalKeyWithMaintainers => Option[ContractId],
  ): Either[Error, A] = {
    @tailrec
    def go(res: Result[A]): Either[Error, A] =
      res match {
        case ResultDone(x) => Right(x)
        case ResultError(err) => Left(err)
        case ResultNeedContract(acoid, resume) => go(resume(pcs(acoid)))
        case ResultNeedPackage(pkgId, resume) => go(resume(packages(pkgId)))
        case ResultNeedKey(key, resume) => go(resume(keys(key)))
      }
    go(this)
  }
}

final case class ResultDone[A](result: A) extends Result[A]
object ResultDone {
  val Unit: ResultDone[Unit] = new ResultDone(())
}
final case class ResultError(err: Error) extends Result[Nothing]

/** Intermediate result indicating that a [[ContractInst]] is required to complete the computation.
  * To resume the computation, the caller must invoke `resume` with the following argument:
  * <ul>
  * <li>`Some(contractInstance)`, if the caller can dereference `acoid` to `contractInstance`</li>
  * <li>`None`, if the caller is unable to dereference `acoid`
  * </ul>
  */
final case class ResultNeedContract[A](
    acoid: ContractId,
    resume: Option[ContractInst[VersionedValue[ContractId]]] => Result[A],
) extends Result[A]

/** Intermediate result indicating that a [[Package]] is required to complete the computation.
  * To resume the computation, the caller must invoke `resume` with the following argument:
  * <ul>
  * <li>`Some(package)`, if the caller can dereference `packageId` to `package`</li>
  * <li>`None`, if the caller is unable to dereference `packageId`
  * </ul>
  */
final case class ResultNeedPackage[A](packageId: PackageId, resume: Option[Package] => Result[A])
    extends Result[A]

final case class ResultNeedKey[A](
    key: GlobalKeyWithMaintainers,
    resume: Option[ContractId] => Result[A],
) extends Result[A]

object Result {
  // fails with ResultError if the package is not found
  private[lf] def needPackage[A](packageId: PackageId, resume: Package => Result[A]) =
    ResultNeedPackage(
      packageId,
      {
        case None => ResultError(Error(s"Couldn't find package $packageId"))
        case Some(pkg) => resume(pkg)
      },
    )

  private[lf] def needContract[A](
      acoid: ContractId,
      resume: ContractInst[VersionedValue[ContractId]] => Result[A],
  ) =
    ResultNeedContract(
      acoid,
      {
        case None => ResultError(ContractNotFound(acoid))
        case Some(contract) => resume(contract)
      },
    )

  def sequence[A](results0: ImmArray[Result[A]]): Result[ImmArray[A]] = {
    @tailrec
    def go(okResults: BackStack[A], results: ImmArray[Result[A]]): Result[BackStack[A]] =
      results match {
        case ImmArray() => ResultDone(okResults)
        case ImmArrayCons(res, results_) =>
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
          }
      }
    go(BackStack.empty, results0).map(_.toImmArray)
  }

  def fromEither[A](err: Either[Error, A]): Result[A] = err match {
    case Left(e) => ResultError(e)
    case Right(x) => ResultDone(x)
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
