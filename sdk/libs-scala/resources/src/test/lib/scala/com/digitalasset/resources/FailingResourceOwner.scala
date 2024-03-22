// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.Future

object FailingResourceOwner {
  def apply[T](): AbstractResourceOwner[TestContext, T] =
    new TestResourceOwner[T](
      Future.failed(new FailingResourceFailedToOpen),
      _ => Future.failed(new TriedToReleaseAFailedResource),
    )

  final class FailingResourceFailedToOpen extends Exception("Something broke!")

  final class TriedToReleaseAFailedResource extends Exception("Tried to release a failed resource.")
}
