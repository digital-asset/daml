// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{BackStack, ImmArray, ImmArrayCons}
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value._
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

  // quick and dirty way to consume a Result
  def consume(
      pcs: AbsoluteContractId => Option[ContractInst[VersionedValue[AbsoluteContractId]]],
      packages: PackageId => Option[Package],
      keys: GlobalKey => Option[AbsoluteContractId]): Either[Error, A] = {
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
final case class ResultError(err: Error) extends Result[Nothing]

/**
  * Intermediate result indicating that a [[ContractInst]] is required to complete the computation.
  * To resume the computation, the caller must invoke `resume` with the following argument:
  * <ul>
  * <li>`Some(contractInstance)`, if the caller can dereference `acoid` to `contractInstance`</li>
  * <li>`None`, if the caller is unable to dereference `acoid`
  * </ul>
  */
final case class ResultNeedContract[A](
    acoid: AbsoluteContractId,
    resume: Option[ContractInst[VersionedValue[AbsoluteContractId]]] => Result[A])
    extends Result[A]

/**
  * Intermediate result indicating that a [[Package]] is required to complete the computation.
  * To resume the computation, the caller must invoke `resume` with the following argument:
  * <ul>
  * <li>`Some(package)`, if the caller can dereference `packageId` to `package`</li>
  * <li>`None`, if the caller is unable to dereference `packageId`
  * </ul>
  */
final case class ResultNeedPackage[A](packageId: PackageId, resume: Option[Package] => Result[A])
    extends Result[A]

final case class ResultNeedKey[A](key: GlobalKey, resume: Option[AbsoluteContractId] => Result[A])
    extends Result[A]

object Result {
  // fails with ResultError if the package is not found
  def needPackage[A](packageId: PackageId, resume: Package => Result[A]) =
    ResultNeedPackage(packageId, {
      case None => ResultError(Error(s"Couldn't find package $packageId"))
      case Some(pkg) => resume(pkg)
    })

  def needPackage[A](
      compiledPackages: ConcurrentCompiledPackages,
      packageId: PackageId,
      resume: Package => Result[A]): Result[A] =
    compiledPackages.getPackage(packageId) match {
      case Some(pkg) => resume(pkg)
      case None =>
        ResultNeedPackage(packageId, {
          case None => ResultError(Error(s"Couldn't find package $packageId"))
          case Some(pkg) =>
            compiledPackages.addPackage(packageId, pkg)
            resume(pkg)
        })
    }

  def needDefinition[A](
      packagesCache: ConcurrentCompiledPackages,
      identifier: Identifier,
      resume: Definition => Result[A]): Result[A] =
    needPackage(
      packagesCache,
      identifier.packageId,
      pkg =>
        fromEither(PackageLookup.lookupDefinition(pkg, identifier.qualifiedName))
          .flatMap(resume)
    )

  def needDataType[A](
      packagesCache: ConcurrentCompiledPackages,
      identifier: Identifier,
      resume: DDataType => Result[A]): Result[A] =
    needPackage(
      packagesCache,
      identifier.packageId,
      pkg =>
        fromEither(PackageLookup.lookupDataType(pkg, identifier.qualifiedName))
          .flatMap(resume)
    )

  def needTemplate[A](
      packagesCache: ConcurrentCompiledPackages,
      identifier: Identifier,
      resume: Template => Result[A]): Result[A] =
    needPackage(
      packagesCache,
      identifier.packageId,
      pkg =>
        fromEither(PackageLookup.lookupTemplate(pkg, identifier.qualifiedName))
          .flatMap(resume)
    )

  def needContract[A](
      acoid: AbsoluteContractId,
      resume: ContractInst[VersionedValue[AbsoluteContractId]] => Result[A]) =
    ResultNeedContract(acoid, {
      case None => ResultError(Error(s"dependency error: couldn't find contract $acoid"))
      case Some(contract) => resume(contract)
    })

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
                  resume(pkg).flatMap(
                    x =>
                      Result
                        .sequence(results_)
                        .map(otherResults => (okResults :+ x) :++ otherResults)))
            case ResultNeedContract(acoid, resume) =>
              ResultNeedContract(
                acoid,
                coinst =>
                  resume(coinst).flatMap(
                    x =>
                      Result
                        .sequence(results_)
                        .map(otherResults => (okResults :+ x) :++ otherResults)))
            case ResultNeedKey(gk, resume) =>
              ResultNeedKey(
                gk,
                mbAcoid =>
                  resume(mbAcoid).flatMap(
                    x =>
                      Result
                        .sequence(results_)
                        .map(otherResults => (okResults :+ x) :++ otherResults)
                )
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
    if (assertion) {
      ResultDone(())
    } else ResultError(err)

  implicit val resultInstance: Monad[Result] = new Monad[Result] {
    override def point[A](a: => A): Result[A] = ResultDone(a)
    override def bind[A, B](fa: Result[A])(f: A => Result[B]): Result[B] = fa.flatMap(f)
  }
}
