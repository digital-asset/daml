// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.damle

import java.util.concurrent.ConcurrentHashMap

import com.digitalasset.daml.lf.engine.{
  Result,
  ResultDone,
  ResultError,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage,
  Error => DamlLfError
}
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * provides smart constructor for DAML engine environment
  */
object SandboxDamle {
  // Concurrent map of promises to request each package only once.
  private[this] val packagePromises: ConcurrentHashMap[Ref.PackageId, Promise[Option[Package]]] =
    new ConcurrentHashMap()

  def consume[A](result: Result[A])(
      getPackage: Ref.PackageId => Future[Option[Package]],
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]],
      lookupKey: GlobalKey => Future[Option[AbsoluteContractId]])(
      implicit ec: ExecutionContext): Future[Either[DamlLfError, A]] = {

    def resolveStep(result: Result[A]): Future[Either[DamlLfError, A]] = {
      result match {
        case ResultNeedPackage(packageId, resume) =>
          var gettingPackage = false
          packagePromises
            .computeIfAbsent(packageId, { _ =>
              val p = Promise[Option[Package]]()
              gettingPackage = true
              getPackage(packageId).foreach(p.success)
              p
            })
            .future
            .flatMap { mbPkg =>
              if (gettingPackage && mbPkg.isEmpty) {
                // Failed to find the package. Remove the promise to allow later retries.
                packagePromises.remove(packageId)
              }
              resolveStep(resume(mbPkg))
            }

        case ResultDone(r) => Future.successful(Right(r))
        case ResultNeedKey(key, resume) =>
          lookupKey(key).flatMap(mbcoid => resolveStep(resume(mbcoid)))
        case ResultNeedContract(acoid, resume) =>
          getContract(acoid).flatMap(o => resolveStep(resume(o)))
        case ResultError(err) => Future.successful(Left(err))
      }
    }

    resolveStep(result)
  }

}
