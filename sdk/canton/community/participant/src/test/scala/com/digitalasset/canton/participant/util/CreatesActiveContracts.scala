// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.daml.ledger
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.platform.apiserver.FatContractInstanceHelper
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{DefaultTestIdentities, PhysicalSynchronizerId}
import com.digitalasset.canton.util.TestContractHasher
import com.digitalasset.canton.{BaseTest, NeedsNewLfContractIds, ReassignmentCounter}
import com.digitalasset.daml.lf

/** Helper that allows unit tests to create active contracts for testing.
  */
private[participant] trait CreatesActiveContracts {
  self: NeedsNewLfContractIds & BaseTest =>

  protected def psid: PhysicalSynchronizerId
  protected def testSymbolicCrypto: SymbolicPureCrypto

  protected def createActiveContract(): ActiveContract = {

    // 1. Create the prerequisites for coming up with an authenticated LAPI active contract.
    val cidUnauthenticated = newLfContractId()
    val contractIdV1Version = CantonContractIdVersion.maxV1
    val unicumGenerator = new UnicumGenerator(testSymbolicCrypto)
    val signatory = DefaultTestIdentities.party1

    // Create an unauthenticated contract first, i.e. without an authenticated contract suffix
    // as input for creating an authenticated contract.
    val unauthenticatedLfFatContract = FatContractInstanceHelper.buildFatContractInstance(
      templateId = lf.data.Ref.Identifier.assertFromString("some:pkg:identifier"),
      packageName = lf.data.Ref.PackageName.assertFromString("pkg-name"),
      contractId = cidUnauthenticated,
      argument = lf.value.Value.ValueNil,
      createdAt = CantonTimestamp.Epoch.underlying,
      authenticationData = ContractAuthenticationDataV1(TestSalt.generateSalt(0))(
        contractIdV1Version
      ).toLfBytes,
      signatories = Set(signatory.toLf),
      stakeholders = Set(signatory.toLf),
      keyOpt = None,
      version = LfSerializationVersion.minVersion,
    )

    val contractId = valueOrFail(
      unicumGenerator
        .recomputeUnicum(
          contractInstance = unauthenticatedLfFatContract,
          cantonContractIdVersion = contractIdV1Version,
          contractHash = TestContractHasher.Sync.hash(
            unauthenticatedLfFatContract.toCreateNode,
            contractIdV1Version.contractHashingMethod,
          ),
        )
        .map(contractIdV1Version.fromDiscriminator(cidUnauthenticated.discriminator, _))
    )("compute unicum and authenticated contract id")

    // Use the authenticated contract to come up with the contract proto serialization.
    val authenticatedFatContractInstance = unauthenticatedLfFatContract.mapCid(_ => contractId)
    val serialization = valueOrFail(
      lf.transaction.TransactionCoder.encodeFatContractInstance(authenticatedFatContractInstance)
    )("serialize contract instance")

    // 2. Create LAPI active contract
    val lapiActiveContract = ledger.api.v2.state_service.ActiveContract(
      createdEvent = Some(
        ledger.api.v2.event.CreatedEvent(
          offset = 0L,
          nodeId = 0,
          contractId = contractId.coid,
          templateId = Some(ledger.api.v2.value.Identifier(M.Cycle.PACKAGE_ID, "Cycle", "Cycle")),
          contractKey = None,
          createArguments = None,
          createdEventBlob = serialization,
          interfaceViews = Seq.empty,
          witnessParties = Seq.empty,
          signatories = Seq(signatory.toProtoPrimitive),
          observers = Seq.empty,
          createdAt = Some(CantonTimestamp.Epoch.toProtoTimestamp),
          packageName = M.Cycle.PACKAGE_ID,
          acsDelta = false,
          representativePackageId = M.Cycle.PACKAGE_ID,
        )
      ),
      synchronizerId = psid.logical.toProtoPrimitive,
      reassignmentCounter = ReassignmentCounter.Genesis.unwrap,
    )

    ActiveContract.create(lapiActiveContract)(testedProtocolVersion)
  }
}
