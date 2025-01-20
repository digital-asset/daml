// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import com.daml.lf.data.Ref.{IdString, PackageId}
import com.digitalasset.canton.*
import com.digitalasset.canton.data.GenTransactionTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  defaultTestingIdentityFactory,
  defaultTestingTopology,
}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
import com.digitalasset.canton.topology.transaction.CheckOnlyPackages
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
final class TransactionTreeFactoryImplTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec {

  val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  def successfulLookup(
      example: ExampleTransaction
  ): SerializableContractOfId = id => {
    EitherT.fromEither(
      example.inputContracts
        .get(id)
        .toRight(ContractLookupError(id, "Unable to lookup input contract from test data"))
    )
  }

  def failedLookup(
      testErrorMessage: String
  ): SerializableContractOfId = id => EitherT.leftT(ContractLookupError(id, testErrorMessage))

  def createTransactionTreeFactory(version: ProtocolVersion): TransactionTreeFactoryImpl =
    TransactionTreeFactoryImpl(
      ExampleTransactionFactory.submitterParticipant,
      factory.domainId,
      version,
      factory.cryptoOps,
      uniqueContractKeys = true,
      loggerFactory,
    )

  def createTransactionTree(
      treeFactory: TransactionTreeFactoryImpl,
      transaction: WellFormedTransaction[WithoutSuffixes],
      contractInstanceOfId: SerializableContractOfId,
      keyResolver: LfKeyResolver,
      actAs: List[LfPartyId] = List(ExampleTransactionFactory.submitter),
      snapshot: TopologySnapshot = factory.topologySnapshot,
  ): EitherT[Future, TransactionTreeConversionError, GenTransactionTree] = {
    val submitterInfo = DefaultParticipantStateValues.submitterInfo(actAs)
    val unsuffixedContractPackages = transaction.unwrap.nodes.values.collect {
      case create: LfNodeCreate => create.coid -> create.templateId.packageId
    }.toMap
    treeFactory
      .createTransactionTree(
        transaction = transaction,
        submitterInfo = submitterInfo,
        confirmationPolicy = factory.confirmationPolicy,
        workflowId = Some(WorkflowId.assertFromString("testWorkflowId")),
        mediator = factory.mediatorRef,
        transactionSeed = factory.transactionSeed,
        transactionUuid = factory.transactionUuid,
        topologySnapshot = snapshot,
        contractOfId = contractInstanceOfId,
        keyResolver = keyResolver,
        maxSequencingTime = factory.ledgerTime.plusSeconds(100),
        contractPackages = unsuffixedContractPackages,
      )
      .failOnShutdownTo(fail("creating tx tree"))
  }

  "TransactionTreeFactoryImpl@testedVersion" should {
    // Shadow default factory with the protocol version explicitly set
    val factory: ExampleTransactionFactory = new ExampleTransactionFactory(
      versionOverride = Some(testedProtocolVersion)
    )()

    def withPkgNotVetted(notVetted: LfPackageId) =
      defaultTestingTopology
        .focus(_.vettedPackages)
        .modify(_.map(_.focus(_.packageIds).modify(_.filterNot(_ == notVetted))))

    "A transaction tree factory" when {

      "everything is ok" must {
        forEvery(factory.standardHappyCases) { example =>
          lazy val treeFactory = createTransactionTreeFactory(testedProtocolVersion)

          s"create the correct views for: $example" in {
            createTransactionTree(
              treeFactory,
              example.wellFormedUnsuffixedTransaction,
              successfulLookup(example),
              example.keyResolver,
            ).value.flatMap(_ should equal(Right(example.transactionTree)))
          }
        }
      }

      "a contract lookup fails" must {
        lazy val errorMessage = "Test error message"
        lazy val treeFactory = createTransactionTreeFactory(testedProtocolVersion)

        lazy val example = factory.SingleExercise(
          factory.deriveNodeSeed(0)
        ) // pick an example that needs divulgence of absolute ids

        "reject the input" in {
          createTransactionTree(
            treeFactory,
            example.wellFormedUnsuffixedTransaction,
            failedLookup(errorMessage),
            example.keyResolver,
          ).value.flatMap(
            _ shouldEqual Left(
              ContractLookupError(example.contractId, errorMessage)
            )
          )
        }
      }

      "empty actAs set is empty" must {
        lazy val treeFactory = createTransactionTreeFactory(testedProtocolVersion)

        "reject the input" in {
          val example = factory.standardHappyCases.headOption.value
          createTransactionTree(
            treeFactory,
            example.wellFormedUnsuffixedTransaction,
            successfulLookup(example),
            example.keyResolver,
            actAs = List.empty,
          ).value
            .flatMap(
              _ should equal(Left(SubmitterMetadataError("The actAs set must not be empty.")))
            )
        }
      }

      "checking package vettings" must {
        lazy val treeFactory = createTransactionTreeFactory(testedProtocolVersion)

        "fail if the main package is not vetted" in {
          val example = factory.standardHappyCases(2)

          createTransactionTree(
            treeFactory,
            example.wellFormedUnsuffixedTransaction,
            successfulLookup(example),
            example.keyResolver,
            snapshot =
              defaultTestingTopology.withVettedPackages(Seq.empty).build().topologySnapshot(),
          ).value.flatMap(_ should matchPattern {
            case Left(
                  PackageStateErrors(
                    Seq(
                      PackageNotVettedBy(
                        ExampleTransactionFactory.packageId,
                        ExampleTransactionFactory.submitterParticipant,
                      ),
                      PackageNotVettedBy(
                        ExampleTransactionFactory.packageId,
                        ExampleTransactionFactory.observerParticipant,
                      ),
                    )
                  )
                ) =>
          })
        }

        "fail if the exercise interface package-id is not vetted" in {
          val example = factory.UpgradedLocalContract

          createTransactionTree(
            treeFactory,
            example.wellFormedUnsuffixedTransaction,
            successfulLookup(example),
            example.keyResolver,
            snapshot = withPkgNotVetted(ExampleTransactionFactory.interfacePackageId)
              .build()
              .topologySnapshot(),
          ).value.flatMap(_ should matchPattern {
            case Left(
                  PackageStateErrors(
                    Seq(
                      PackageNotVettedBy(
                        ExampleTransactionFactory.interfacePackageId,
                        ExampleTransactionFactory.submitterParticipant,
                      ),
                      PackageNotVettedBy(
                        ExampleTransactionFactory.interfacePackageId,
                        ExampleTransactionFactory.extraParticipant,
                      ),
                    )
                  )
                ) =>
          })
        }

        "fail if the exercise interface package-id is not vetted but check-only" in {
          val example = factory.UpgradedLocalContract

          createTransactionTree(
            treeFactory,
            example.wellFormedUnsuffixedTransaction,
            successfulLookup(example),
            example.keyResolver,
            snapshot = withPkgNotVetted(ExampleTransactionFactory.interfacePackageId)
              .withCheckOnlyPackages(
                Seq(
                  CheckOnlyPackages(
                    ExampleTransactionFactory.submitterParticipant,
                    Seq(ExampleTransactionFactory.interfacePackageId),
                  ),
                  CheckOnlyPackages(
                    ExampleTransactionFactory.extraParticipant,
                    Seq(ExampleTransactionFactory.interfacePackageId),
                  ),
                )
              )
              .build()
              .topologySnapshot(),
          ).value.flatMap(_ should matchPattern {
            case Left(
                  PackageStateErrors(
                    Seq(
                      PackageNotVettedBy(
                        ExampleTransactionFactory.interfacePackageId,
                        ExampleTransactionFactory.submitterParticipant,
                      ),
                      PackageNotVettedBy(
                        ExampleTransactionFactory.interfacePackageId,
                        ExampleTransactionFactory.extraParticipant,
                      ),
                    )
                  )
                ) =>
          })
        }

        "fail if some dependency is not vetted" onlyRunWithOrLowerThan ProtocolVersion.v5 in {
          val example = factory.standardHappyCases(2)
          for {
            err <- createTransactionTree(
              treeFactory,
              example.wellFormedUnsuffixedTransaction,
              successfulLookup(example),
              example.keyResolver,
              snapshot = defaultTestingIdentityFactory.topologySnapshot(
                packageDependencies = TestPackageDependencyResolver.WithUnvettedDep
              ),
            ).value
          } yield inside(err) {
            case Left(
                  PackageStateErrors(
                    Seq(
                      PackageNotVettedBy(
                        TestPackageDependencyResolver.exampleDependency,
                        ExampleTransactionFactory.submitterParticipant,
                      ),
                      PackageNotVettedBy(
                        TestPackageDependencyResolver.exampleDependency,
                        ExampleTransactionFactory.observerParticipant,
                      ),
                    )
                  )
                ) =>
              succeed
          }
        }

        "fail if some dependency does not meet the package topology requirements" onlyRunWithOrGreaterThan ProtocolVersion.v7 in {
          val example = factory.UpgradedLocalContract
          for {
            err <- createTransactionTree(
              treeFactory,
              example.wellFormedUnsuffixedTransaction,
              successfulLookup(example),
              example.keyResolver,
              snapshot = defaultTestingIdentityFactory.topologySnapshot(
                packageDependencies = TestPackageDependencyResolver.WithUnvettedDep
              ),
            ).value
          } yield inside(err) {
            case Left(
                  PackageStateErrors(
                    Seq(
                      PackageNotVettedBy(
                        TestPackageDependencyResolver.exampleDependency,
                        ExampleTransactionFactory.submitterParticipant,
                      ),
                      PackageNotDeclaredCheckOnlyBy(
                        TestPackageDependencyResolver.exampleDependency,
                        ExampleTransactionFactory.extraParticipant,
                      ),
                    )
                  )
                ) =>
              succeed
          }
        }

        "fail if the input contract creation package-id is not marked at least check-only" onlyRunWithOrGreaterThan ProtocolVersion.v7 in {
          val example = factory.UpgradedLocalContract

          createTransactionTree(
            treeFactory,
            example.wellFormedUnsuffixedTransaction,
            successfulLookup(example),
            example.keyResolver,
            // Do not vet the package-id of the locally created contract
            snapshot =
              withPkgNotVetted(ExampleTransactionFactory.packageId).build().topologySnapshot(),
          ).value.flatMap(_ should matchPattern {
            case Left(
                  PackageStateErrors(
                    Seq(
                      PackageNotVettedBy(
                        ExampleTransactionFactory.packageId,
                        ExampleTransactionFactory.submitterParticipant,
                      ),
                      PackageNotDeclaredCheckOnlyBy(
                        ExampleTransactionFactory.packageId,
                        ExampleTransactionFactory.extraParticipant,
                      ),
                    )
                  )
                ) =>
          })
        }

        "fail if the input contract creation package-id is not vetted but only marked check-only" onlyRunWithOrGreaterThan ProtocolVersion.v7 in {
          val example = factory.UpgradedLocalContract

          createTransactionTree(
            treeFactory,
            example.wellFormedUnsuffixedTransaction,
            successfulLookup(example),
            example.keyResolver,
            snapshot = defaultTestingTopology
              .focus(_.vettedPackages)
              .modify(
                _.map(
                  _.focus(_.packageIds).modify(
                    // Do not vet the package-id of the locally created contract,
                    // but only have it check-only for both participants
                    _.filterNot(_ == ExampleTransactionFactory.packageId)
                  )
                )
              )
              .withCheckOnlyPackages(
                Seq(
                  CheckOnlyPackages(
                    ExampleTransactionFactory.submitterParticipant,
                    Seq(ExampleTransactionFactory.packageId),
                  ),
                  CheckOnlyPackages(
                    ExampleTransactionFactory.extraParticipant,
                    Seq(ExampleTransactionFactory.packageId),
                  ),
                )
              )
              .build()
              .topologySnapshot(),
          ).value.flatMap(_ should matchPattern {
            case Left(
                  PackageStateErrors(
                    Seq(
                      PackageNotVettedBy(
                        ExampleTransactionFactory.packageId,
                        ExampleTransactionFactory.submitterParticipant,
                      )
                    )
                  )
                ) =>
          })
        }

        "fail gracefully if the present participant is misconfigured and somehow doesn't have a package that it should have" in {
          val example = factory.UpgradedLocalContract

          for {
            err <- createTransactionTree(
              treeFactory,
              example.wellFormedUnsuffixedTransaction,
              successfulLookup(example),
              example.keyResolver,
              snapshot = defaultTestingIdentityFactory.topologySnapshot(
                packageDependencies = TestPackageDependencyResolver.WithUnknownPkg
              ),
            ).value
          } yield inside(err) {
            case Left(
                  PackageStateErrors(
                    Seq(
                      PackageUnknownTo(
                        ExampleTransactionFactory.packageId,
                        ExampleTransactionFactory.submitterParticipant,
                      ),
                      PackageUnknownTo(
                        ExampleTransactionFactory.packageId,
                        ExampleTransactionFactory.extraParticipant,
                      ),
                    )
                  )
                ) =>
              succeed
          }
        }
      }
    }
  }

  object TestPackageDependencyResolver {

    import cats.syntax.either.*

    val exampleDependency: IdString.PackageId = PackageId.assertFromString("example-dependency")

    object WithUnvettedDep extends PackageDependencyResolverUS {
      override def packageDependencies(packages: List[PackageId])(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
        packages match {
          case ExampleTransactionFactory.packageId :: Nil =>
            Right(Set(exampleDependency)).toEitherT[FutureUnlessShutdown]
          case _ => Right(Set.empty[PackageId]).toEitherT[FutureUnlessShutdown]
        }
    }

    object WithUnknownPkg extends PackageDependencyResolverUS {
      override def packageDependencies(packages: List[PackageId])(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
        packages match {
          case ExampleTransactionFactory.packageId :: Nil =>
            Left(ExampleTransactionFactory.packageId).toEitherT[FutureUnlessShutdown]
          case _ => Right(Set.empty[PackageId]).toEitherT[FutureUnlessShutdown]
        }
    }
  }
}
