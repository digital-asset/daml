// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.v1.transaction_service.GetTransactionsResponse
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.validation.ValueValidator
import com.daml.ledger.offset.Offset
import com.daml.lf.engine.{
  Engine,
  Result,
  ResultDone,
  ResultError,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage,
}
import com.daml.lf.transaction.Versioned
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.cache.{PackageMetadataCache, SingletonPackageMetadataCache}
import io.grpc.Status.Code

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

private[platform] case class TransactionFilter(
    // For each party, the set of templates that should be included in the result
    // If the set is empty, all templates should be included
    // TODO DPP-1068: better representation of wildcard filters? Some ADT instead of Set.empty?
    templatesToInclude: Map[Ref.Party, Set[Ref.Identifier]],

    // For each template, the set of interfaces for which the view needs to be computed
    // If the set is empty, no views should be computed
    viewsToCompute: Map[Ref.Identifier, Set[Ref.Identifier]],
)

// TODO DPP-1068: Add unit tests for this
private[platform] object TransactionFilter {

  def apply(
      endInclusive: Offset,
      packageMetadataCache: PackageMetadataCache,
      filter: domain.TransactionFilter,
  ): TransactionFilter = {

    val listOfViewsToCompute: ListBuffer[(Ref.Identifier, Ref.Identifier)] = ListBuffer.empty

    val templatesToInclude = filter.filtersByParty.view
      .mapValues(filters =>
        filters.inclusive.fold(
          Set.empty[Ref.Identifier]
        )(inclusiveFilters => {
          val interfaceTemplateIds = inclusiveFilters.interfaceFilters.flatMap(interfaceFilter => {
            val interfaceId = interfaceFilter.interfaceId

            // Check whether the interface is known
            // TODO DPP-1068: compare interfaceOffset with the ledger end queried before this call.
            //  If the offset is higher than current ledger end, it means the package defining the interface
            //  was uploaded right now, and the cache might only contain partial data for this interface.
            //  SingletonPackageMetadataCached doesn't have this problem as it updates its state atomically,
            //  but other implementations might not.
            val interfaceOffset = packageMetadataCache.interfaceAddedAt(interfaceId)
            if (interfaceOffset.isEmpty) {
              // TODO DPP-1068: Proper error handling, maybe return Either[Error, TransactionFilter]
              throw new RuntimeException(
                s"Interface $interfaceId is not known"
              )
            }

            // Find all templates that implement this interface
            val templateIds = packageMetadataCache.getInterfaceImplementations(interfaceId)

            // Remember for each of these templates whether we have to compute the view
            if (interfaceFilter.includeView) {
              templateIds.foreach(tid => listOfViewsToCompute.addOne(tid -> interfaceId))
            }

            templateIds
          })

          val allTemplateIds = inclusiveFilters.templateIds ++ interfaceTemplateIds

          // Optimization: filter out templates that were introduced after endInclusive,
          // as there will be no contracts for those templates in the given offset range.
          val relevantTemplateIds = allTemplateIds.filter(id =>
            packageMetadataCache.templateAddedAt(id).exists(o => o <= endInclusive)
          )

          relevantTemplateIds
        })
      )
      .toMap // Note: force the computation, otherwise listOfViewsToCompute will be empty in the next step

    val viewsToCompute = listOfViewsToCompute
      .groupBy(_._1)
      .view
      .mapValues(x => x.map(_._2).toSet)
      .toMap

    TransactionFilter(
      templatesToInclude = templatesToInclude,
      viewsToCompute = viewsToCompute,
    )
  }

  def computeInterfaceViews(
      respose: GetTransactionsResponse,
      filter: TransactionFilter,
      engine: Engine,
  )(implicit loggingContext: LoggingContext): GetTransactionsResponse = {
    val transactionsWithViews =
      respose.transactions.map(tx => computeInterfaceViews(tx, filter, engine))
    respose.update(_.transactions := transactionsWithViews)
  }

  /** Returns a copy of the given transaction that contains all required interface views */
  private def computeInterfaceViews(
      transaction: Transaction,
      filter: TransactionFilter,
      engine: Engine,
  )(implicit loggingContext: LoggingContext): Transaction = {
    transaction.update(
      _.events := transaction.events.map(outerEvent =>
        outerEvent.event match {
          case com.daml.ledger.api.v1.event.Event.Event.Created(created) =>
            val templateId = apiIdentifierToDamlLfIdentifier(created.templateId.get)
            val viewsToInclude = filter.viewsToCompute.getOrElse(templateId, Set.empty)
            if (viewsToInclude.isEmpty) {
              outerEvent
            } else {
              val interfaceViews = viewsToInclude
                .map(iid =>
                  computeInterfaceView(templateId, created.createArguments.get, iid, engine)
                )
                .toSeq
              com.daml.ledger.api.v1.event.Event.of(
                com.daml.ledger.api.v1.event.Event.Event
                  .Created(created.update(_.interfaceViews := interfaceViews))
              )
            }

          case _ => outerEvent
        }
      )
    )
  }

  /** Computes all required interface views for the given template */
  private def computeInterfaceView(
      templateId: Ref.Identifier,
      record: com.daml.ledger.api.v1.value.Record,
      interfaceId: Ref.Identifier,
      engine: Engine,
  )(implicit loggingContext: LoggingContext): com.daml.ledger.api.v1.event.InterfaceView = {
    // TODO DPP-1068: The transaction stream contains protobuf-serialized transactions (Source[GetTransactionsResponse, NotUsed]),
    //   we don't have access to the original Daml-LF value.
    //   Here we deserialize the contract argument, use it in the engine, and then serialize it back. This needs to be improved.
    val value = ValueValidator
      .validateRecord(record)(
        DamlContextualizedErrorLogger.forTesting(getClass)
      )
      .getOrElse(throw new RuntimeException("This should never fail"))

    @tailrec
    def go(res: Result[Versioned[Value]]): Either[String, Versioned[Value]] =
      res match {
        case ResultDone(x) => Right(x)
        case ResultError(err) => Left(err.message)
        // Note: the compiler should enforce that the computation is a pure function,
        // ResultNeedContract and ResultNeedKey should never appear in the result.
        case ResultNeedContract(_, _) => Left("View computation must be a pure function")
        case ResultNeedKey(_, _) => Left("View computation must be a pure function")
        case ResultNeedPackage(pkgId, resume) =>
          // TODO DPP-1068: Package loading makes the view computation asynchronous, which is annoying to deal with (see LfValueTranslation).
          //   Here we rely on the package metadata cache to always contain all decoded packages that exist on this participant,
          //   so that we can fetch the decoded package synchronously.
          go(resume(SingletonPackageMetadataCache.getPackage(pkgId)))
      }
    val result = go(engine.computeInterfaceView(templateId, value, interfaceId))

    result
      .flatMap(versionedValue =>
        LfEngineToApi.lfValueToApiRecord(
          verbose = false,
          recordValue = versionedValue.unversioned,
        )
      )
      .fold(
        error =>
          // Note: the view computation is an arbitrary Daml function and can thus fail (e.g., with a Daml exception)
          com.daml.ledger.api.v1.event.InterfaceView(
            interfaceId = Some(LfEngineToApi.toApiIdentifier(interfaceId)),
            // TODO DPP-1068: Use a proper error status
            viewStatus =
              Some(com.google.rpc.status.Status.of(Code.INTERNAL.value(), error, Seq.empty)),
            viewValue = None,
          ),
        value =>
          com.daml.ledger.api.v1.event.InterfaceView(
            interfaceId = Some(LfEngineToApi.toApiIdentifier(interfaceId)),
            viewStatus = Some(com.google.rpc.status.Status.of(0, "", Seq.empty)),
            viewValue = Some(value),
          ),
      )
  }

  // TODO DPP-1068: Copied from LfValueSerialization
  private def apiIdentifierToDamlLfIdentifier(
      id: com.daml.ledger.api.v1.value.Identifier
  ): Ref.Identifier =
    Ref.Identifier(
      Ref.PackageId.assertFromString(id.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(id.moduleName),
        Ref.DottedName.assertFromString(id.entityName),
      ),
    )
}
