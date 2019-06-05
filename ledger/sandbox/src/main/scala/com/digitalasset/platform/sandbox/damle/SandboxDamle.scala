// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.damle

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
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

/**
  * provides smart constructor for DAML engine environment
  */
object SandboxDamle {

  def consume[A](result: Result[A])(
      getPackage: Ref.PackageId => Future[Option[Package]],
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]],
      lookupKey: GlobalKey => Future[Option[AbsoluteContractId]])(
      implicit ec: ExecutionContext): Future[Either[DamlLfError, A]] = {

    def resolveStep(result: Result[A]): Future[Either[DamlLfError, A]] = {
      result match {
        case ResultNeedPackage(packageId, resume) =>
          getPackage(packageId).flatMap(mbPkg => resolveStep(resume(mbPkg)))
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
