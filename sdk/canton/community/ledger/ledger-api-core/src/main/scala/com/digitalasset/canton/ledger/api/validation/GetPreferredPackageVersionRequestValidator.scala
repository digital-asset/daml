// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.implicits.toTraverseOps
import com.daml.ledger.api.v2.interactive.interactive_submission_service.GetPreferredPackageVersionRequest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ContextualizedErrorLogger
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref.{PackageName, Party}
import io.grpc.StatusRuntimeException

object GetPreferredPackageVersionRequestValidator {
  def validate(
      request: GetPreferredPackageVersionRequest
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Either[
    StatusRuntimeException,
    (Set[Party], PackageName, Option[SynchronizerId], Option[CantonTimestamp]),
  ] =
    for {
      nonEmptyRawParties <- FieldValidator.requireNonEmpty(
        s = request.parties,
        fieldName = "parties",
      )
      parties <- FieldValidator.requireParties(nonEmptyRawParties.toSet)
      packageName <- FieldValidator
        .requirePackageName(s = request.packageName, fieldName = "package_name/packageName")
      synchronizerIdO <- FieldValidator.optionalSynchronizerId(
        s = request.synchronizerId,
        fieldName = "synchronizer_id/synchronizerId",
      )
      vettingValidAtO <- request.vettingValidAt
        .traverse(
          CantonTimestamp
            .fromProtoTimestamp(_)
            .left
            .map(protoDeserializationError =>
              ValidationErrors.invalidField(
                fieldName = "vetting_valid_at/vettingValidAt",
                message = protoDeserializationError.message,
              )
            )
        )
    } yield (parties, packageName, synchronizerIdO, vettingValidAtO)
}
