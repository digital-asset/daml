// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.admin.PackageUploader

import scala.concurrent.ExecutionContext

trait PackageUploaderFactory {
  def create(
      createAndInitialize: () => FutureUnlessShutdown[PackageUploader]
  )(implicit
      executionContext: ExecutionContext
  ): FutureUnlessShutdown[Eval[PackageUploader]]
}

object PackageUploaderFactory extends PackageUploaderFactory {
  override def create(
      createAndInitialize: () => FutureUnlessShutdown[PackageUploader]
  )(implicit
      executionContext: ExecutionContext
  ): FutureUnlessShutdown[Eval[PackageUploader]] =
    createAndInitialize().map(Eval.now)
}
