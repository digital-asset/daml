// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.SubmissionConfig
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
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
    val observersPartySetParties: Map[String, List[String]] =
      config.observerPartySets.map { partySet =>
        val parties = names
          .partySetPartyName(
            prefix = partySet.partyNamePrefix,
            numberOfParties = partySet.count,
            uniqueParties = config.uniqueParties,
          )
          .toList
        partySet.partyNamePrefix -> parties
      }.toMap
    logger.info("Allocating parties...")
    for {
      known <- lookupExistingParties()
      signatory <- allocateSignatoryParty(known)
      observers <- allocateParties(observerPartyNames, known)
      divulgees <- allocateParties(divulgeePartyNames, known)
      extraSubmitters <- allocateParties(extraSubmittersPartyNames, known)
      partySetNames = observersPartySetParties.keys
      partySetParties: Map[String, List[Party]] <- Future
        .sequence(partySetNames.map { partySetName =>
          allocateParties(observersPartySetParties(partySetName), known).map(partySetName -> _)
        })
        .map(_.toMap)
    } yield {
      logger.info("Allocating parties completed")
      AllocatedParties(
        signatoryO = Some(signatory),
        observers = observers,
        divulgees = divulgees,
        extraSubmitters = extraSubmitters,
        observerPartySets = partySetParties.view.map { case (partyName, parties) =>
          AllocatedPartySet(
            mainPartyNamePrefix = partyName,
            parties = parties,
          )
        }.toList,
      )
    }
  }

  def lookupExistingParties()(implicit ec: ExecutionContext): Future[Set[String]] =
    adminServices.partyManagementService.listKnownParties()

  private def allocateSignatoryParty(known: Set[String])(implicit
      ec: ExecutionContext
  ): Future[Party] =
    lookupOrAllocateParty(names.signatoryPartyName, known)

  private def allocateParties(partyNames: Seq[String], known: Set[String])(implicit
      ec: ExecutionContext
  ): Future[List[Party]] =
    Future.traverse(partyNames.toList)(lookupOrAllocateParty(_, known))

  private def lookupOrAllocateParty(party: String, known: Set[String])(implicit
      ec: ExecutionContext
  ): Future[Party] =
    if (known.exists(_.startsWith(party))) {
      val partyId = known
        .find(_.startsWith(party))
        .getOrElse(
          throw new RuntimeException(s"Party id for party $party should have been found")
        )
      logger.info(
        s"Found known party: $party with party id: $partyId."
      )
      Future.successful(new Party(partyId))
    } else
      adminServices.partyManagementService.allocateParty(party)

}
