// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.UsableDomain
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

private[transfer] object TransferKnownAndVetted {

  def apply(
      stakeholders: Set[LfPartyId],
      targetTopology: TopologySnapshot,
      contractId: ContractId,
      packageId: PackageId,
      targetDomain: TargetDomainId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    // `checkPackagesVetted` is slightly too generic to check individual contracts but it will
    // become useful when we allow to reassign more than one contract at once
    UsableDomain
      .resolveParticipantsAndCheckPackagesVetted(
        targetDomain.unwrap,
        targetTopology,
        stakeholders.view.map(_ -> Set(packageId)).toMap,
      )
      .leftMap(unknownPackage =>
        TransferOutProcessorError.PackageIdUnknownOrUnvetted(contractId, unknownPackage.unknownTo)
      )
      .leftWiden[TransferProcessorError]

}
