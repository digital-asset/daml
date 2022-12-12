// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.SubmissionConfig
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.client.binding.Primitive
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class PartyAllocating(
    names: Names,
    adminServices: LedgerApiServices,
) {

  private val logger = LoggerFactory.getLogger(getClass)

  def allocateParties(config: SubmissionConfig)(implicit
      ec: ExecutionContext
  ): Future[AllocatedParties] = {
    val observerPartyNames =
      names.observerPartyNames(config.numberOfObservers, config.uniqueParties)
    val divulgeePartyNames =
      names.divulgeePartyNames(config.numberOfDivulgees, config.uniqueParties)
    val extraSubmittersPartyNames =
      names.extraSubmitterPartyNames(config.numberOfExtraSubmitters, config.uniqueParties)
    val observersPartySetParties = {
      config.observerPartySetO.fold(
        List.empty[String]
      )(partySet =>
        names
          .partyNames(
            prefix = partySet.partyNamePrefix,
            numberOfParties = partySet.count,
            uniqueParties = config.uniqueParties,
          )
          .toList
      )
    }
    logger.info("Allocating parties...")
    for {
      known <- lookupExistingParties()
      signatory <- allocateSignatoryParty(known)
      observers <- allocateParties(observerPartyNames, known)
      divulgees <- allocateParties(divulgeePartyNames, known)
      extraSubmitters <- allocateParties(extraSubmittersPartyNames, known)
      partySetParties <- allocateParties(observersPartySetParties, known)
    } yield {
      logger.info("Allocating parties completed")
      AllocatedParties(
        signatoryO = Some(signatory),
        observers = observers,
        divulgees = divulgees,
        extraSubmitters = extraSubmitters,
        observerPartySetO = config.observerPartySetO.map(partySetConfig =>
          AllocatedPartySet(
            partyNamePrefix = partySetConfig.partyNamePrefix,
            parties = partySetParties,
          )
        ),
      )
    }
  }

  def lookupExistingParties()(implicit ec: ExecutionContext): Future[Set[String]] = {
    adminServices.partyManagementService.listKnownParties()
  }

  private def allocateSignatoryParty(known: Set[String])(implicit
      ec: ExecutionContext
  ): Future[Primitive.Party] =
    lookupOrAllocateParty(names.signatoryPartyName, known)

  private def allocateParties(partyNames: Seq[String], known: Set[String])(implicit
      ec: ExecutionContext
  ): Future[List[Primitive.Party]] = {
    Future.traverse(partyNames.toList)(lookupOrAllocateParty(_, known))
  }

  private def lookupOrAllocateParty(party: String, known: Set[String])(implicit
      ec: ExecutionContext
  ): Future[Primitive.Party] = {
    if (known.contains(party)) {
      logger.info(s"Found known party: $party")
      Future.successful(Primitive.Party(party))
    } else
      adminServices.partyManagementService.allocateParty(party)
  }

}
