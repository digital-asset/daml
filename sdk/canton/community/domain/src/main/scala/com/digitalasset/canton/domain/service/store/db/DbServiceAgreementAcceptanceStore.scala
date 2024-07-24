// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.domain.service.store.ServiceAgreementAcceptanceStore
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import slick.jdbc.SetParameter

import scala.concurrent.Future

class DbServiceAgreementAcceptanceStore(
    storage: DbStorage,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends ServiceAgreementAcceptanceStore
    with NamedLogging
    with FlagCloseable
    with HasCloseContext {

  import storage.api.*
  import storage.converters.*

  private implicit val setParameterSignature: SetParameter[Signature] =
    Signature.getVersionedSetParameter(protocolVersion)

  override def insertAcceptance(acceptance: ServiceAgreementAcceptance)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {

    val insertQuery =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sqlu"""insert /*+  ignore_row_on_dupkey_index ( service_agreement_acceptances ( agreement_id, participant_id ) ) */
                into service_agreement_acceptances
                 (agreement_id, participant_id, signature, ts)
                values (${acceptance.agreementId}, ${acceptance.participantId}, ${acceptance.signature}, ${acceptance.timestamp})"""
        case _ =>
          sqlu"""insert into service_agreement_acceptances(agreement_id, participant_id, signature, ts)
                 values (${acceptance.agreementId}, ${acceptance.participantId}, ${acceptance.signature}, ${acceptance.timestamp})
                 on conflict do nothing"""
      }

    storage.update_(insertQuery, functionFullName)
  }

  override def listAcceptances()(implicit
      traceContext: TraceContext
  ): Future[Seq[ServiceAgreementAcceptance]] =
    storage.query(
      sql"select agreement_id, participant_id, signature, ts from service_agreement_acceptances"
        .as[ServiceAgreementAcceptance],
      functionFullName,
    )
}
