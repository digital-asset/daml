// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{LfHash, LfNodeCreate}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.crypto.Hash.HashingMethod

import scala.concurrent.ExecutionContext

object TestContractHasher {

  trait SyncContractHasher {
    def hash(create: LfNodeCreate, hashingMethod: HashingMethod): LfHash
  }

  object Async extends ContractHasher {
    override def hash(create: LfNodeCreate, hashingMethod: HashingMethod)(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, LfHash] =
      EitherT.pure[FutureUnlessShutdown, String](hashInternal(create, hashingMethod))
  }

  object Sync extends SyncContractHasher {
    def hash(create: LfNodeCreate, hashingMethod: HashingMethod): LfHash =
      hashInternal(create, hashingMethod)
  }

  private def hashCreate(
      create: LfNodeCreate,
      upgradeFriendly: Boolean,
  ): LfHash =
    LfHash.assertHashContractInstance(
      create.templateId,
      create.arg,
      create.packageName,
      upgradeFriendly,
    )

  private def hashInternal(create: LfNodeCreate, hashingMethod: HashingMethod): Hash =
    hashingMethod match {

      case HashingMethod.Legacy =>
        hashCreate(create, upgradeFriendly = false)

      case HashingMethod.UpgradeFriendly =>
        hashCreate(create, upgradeFriendly = true)

      /** To calculate the TypeNormal hash requires that the template package is available
        */
      case HashingMethod.TypedNormalForm =>
        val md = MessageDigestPrototype.Sha256.newDigest
        md.update(hashCreate(create, upgradeFriendly = true).bytes.toByteArray)
        md.update("TypedNormalForm".getBytes)
        LfHash.assertFromByteArray(md.digest())

    }

}
