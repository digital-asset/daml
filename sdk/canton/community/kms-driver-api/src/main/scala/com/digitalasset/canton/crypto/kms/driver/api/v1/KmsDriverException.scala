// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.api.v1

/** An exception that should be thrown by the KMS driver in case of failure.
  *
  * @param exception The underlying exception of the KMS that lead to the failure.
  * @param retryable If true the caller can retry the failing operation.
  */
final case class KmsDriverException(exception: Throwable, retryable: Boolean)
    extends RuntimeException(
      s"KMS Driver exception (retryable=$retryable): ${exception.getMessage}",
      exception,
    )
