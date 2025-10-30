// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

object Errors {

  sealed abstract class FrameworkException(message: String, cause: Throwable)
      extends RuntimeException(message, cause)

  final class ParticipantConnectionException(cause: Throwable)
      extends FrameworkException(
        s"Could not connect to the participant: ${cause.getMessage}",
        cause,
      )

  final class DarUploadException(name: String, cause: Throwable)
      extends FrameworkException(s"""Failed to upload DAR "$name": ${cause.getMessage}""", cause)

}
