// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.functor.*
import com.digitalasset.canton.data.TransactionView
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessSet,
}
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{LfKeyResolver, LfPartyId}

final case class UsedAndCreated(
    contracts: UsedAndCreatedContracts,
    keys: InputAndUpdatedKeys,
    hostedWitnesses: Set[LfPartyId],
) {
  def activenessSet: ActivenessSet =
    ActivenessSet(
      contracts = contracts.activenessCheck,
      transferIds = Set.empty,
      keys = keys.activenessCheck,
    )
}

final case class UsedAndCreatedContracts(
    witnessedAndDivulged: Map[LfContractId, SerializableContract],
    checkActivenessTxInputs: Set[LfContractId],
    consumedInputsOfHostedStakeholders: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    used: Map[LfContractId, SerializableContract],
    maybeCreated: Map[LfContractId, Option[SerializableContract]],
    transient: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
) {
  def activenessCheck: ActivenessCheck[LfContractId] =
    ActivenessCheck.tryCreate(
      checkFresh = maybeCreated.keySet,
      checkFree = Set.empty,
      checkActive = checkActivenessTxInputs,
      lock = consumedInputsOfHostedStakeholders.keySet ++ created.keySet,
      needPriorState = Set.empty,
    )

  def created: Map[LfContractId, SerializableContract] = maybeCreated.collect {
    case (cid, Some(sc)) => cid -> sc
  }
}

trait InputAndUpdatedKeys extends PrettyPrinting {

  /** A key resolver that is suitable for reinterpreting the given root view. */
  def keyResolverFor(rootView: TransactionView)(implicit
      loggingContext: NamedLoggingContext
  ): LfKeyResolver

  /** Keys that must be free before executing the transaction */
  def uckFreeKeysOfHostedMaintainers: Set[LfGlobalKey]

  /** Keys that will be updated by the transaction.
    * The value indicates the new status after the transaction.
    */
  def uckUpdatedKeysOfHostedMaintainers: Map[LfGlobalKey, ContractKeyJournal.Status]

  def activenessCheck: ActivenessCheck[LfGlobalKey] =
    ActivenessCheck.tryCreate(
      checkFresh = Set.empty,
      checkFree = uckFreeKeysOfHostedMaintainers,
      checkActive = Set.empty,
      lock = uckUpdatedKeysOfHostedMaintainers.keySet,
      needPriorState = Set.empty,
    )
}

/** @param keyResolvers The key resolvers for the root views with the given hashes */
final case class InputAndUpdatedKeysV2(
    keyResolvers: Map[ViewHash, LfKeyResolver],
    override val uckFreeKeysOfHostedMaintainers: Set[LfGlobalKey],
    override val uckUpdatedKeysOfHostedMaintainers: Map[LfGlobalKey, ContractKeyJournal.Status],
) extends InputAndUpdatedKeys
    with HasLoggerName {

  /** @throws java.lang.IllegalArgumentException if the root view is not a root view of the projection */
  override def keyResolverFor(
      rootView: TransactionView
  )(implicit loggingContext: NamedLoggingContext): LfKeyResolver = keyResolvers.getOrElse(
    rootView.viewHash,
    ErrorUtil.internalError(new IllegalArgumentException(s"Unknown root view hash $rootView")),
  )

  override def pretty: Pretty[InputAndUpdatedKeysV2] = prettyOfClass(
    param("key resolvers", _.keyResolvers.toSeq),
    paramIfNonEmpty("uck free keys of hosted maintainers", _.uckFreeKeysOfHostedMaintainers),
    paramIfNonEmpty("uck updated keys of hosted maintainers", _.uckUpdatedKeysOfHostedMaintainers),
  )
}

final case class InputAndUpdatedKeysV3(
    override val uckFreeKeysOfHostedMaintainers: Set[LfGlobalKey],
    override val uckUpdatedKeysOfHostedMaintainers: Map[LfGlobalKey, ContractKeyJournal.Status],
) extends InputAndUpdatedKeys {

  override def keyResolverFor(rootView: TransactionView)(implicit
      loggingContext: NamedLoggingContext
  ): LfKeyResolver = rootView.globalKeyInputs.fmap(_.resolution)

  override def pretty: Pretty[InputAndUpdatedKeysV3] = prettyOfClass(
    paramIfNonEmpty("uck free keys of hosted maintainers", _.uckFreeKeysOfHostedMaintainers),
    paramIfNonEmpty("uck updated keys of hosted maintainers", _.uckUpdatedKeysOfHostedMaintainers),
  )
}
