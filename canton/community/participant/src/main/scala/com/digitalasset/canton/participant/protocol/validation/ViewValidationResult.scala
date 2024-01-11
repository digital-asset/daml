// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ParticipantTransactionView
import com.digitalasset.canton.protocol.{LfContractId, LfGlobalKey}

final case class ViewValidationResult(
    view: ParticipantTransactionView,
    activenessResult: ViewActivenessResult,
)

/** The result of the activeness check for a view
  *
  * @param inactiveContracts The input contracts that are inactive
  * @param alreadyLockedContracts The contracts that are already locked
  * @param existingContracts The created contracts that already exist
  * @param duplicateKeys Keys of created contracts that are already assigned
  * @param inconsistentKeys Contract keys that should be free (in terms of lookupByKey), but are not
  * @param lockedKeys Keys that are already locked
  */
final case class ViewActivenessResult(
    inactiveContracts: Set[LfContractId],
    alreadyLockedContracts: Set[LfContractId],
    existingContracts: Set[LfContractId],
    duplicateKeys: Map[LfGlobalKey, Set[LfPartyId]],
    inconsistentKeys: Map[LfGlobalKey, Set[LfPartyId]],
    lockedKeys: Map[LfGlobalKey, Set[LfPartyId]],
)
