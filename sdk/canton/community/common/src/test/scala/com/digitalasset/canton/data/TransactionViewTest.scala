// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.crypto.{HashOps, Salt, TestSalt}
import com.digitalasset.canton.data.ViewParticipantData.InvalidViewParticipantData
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.LfTransactionBuilder
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPackageId, LfVersioned}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AnyWordSpec

class TransactionViewTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val factory = new ExampleTransactionFactory()()

  private val hashOps: HashOps = factory.cryptoOps

  private val contractInst: LfContractInst = ExampleTransactionFactory.contractInstance()

  private val cantonContractIdVersion: CantonContractIdVersion = AuthenticatedContractIdVersionV10
  private val createdId: LfContractId =
    cantonContractIdVersion.fromDiscriminator(
      ExampleTransactionFactory.lfHash(3),
      ExampleTransactionFactory.unicum(0),
    )
  private val absoluteId: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  private val otherAbsoluteId: LfContractId = ExampleTransactionFactory.suffixedId(1, 1)
  private val salt: Salt = factory.transactionSalt
  private val nodeSeed: LfHash = ExampleTransactionFactory.lfHash(1)
  private val globalKey: LfGlobalKey =
    LfGlobalKey
      .build(
        LfTransactionBuilder.defaultTemplateId,
        Value.ValueInt64(100L),
        LfTransactionBuilder.defaultPackageName,
      )
      .value

  private val defaultPackagePreference = Set(ExampleTransactionFactory.packageId)

  private val defaultActionDescription: ActionDescription =
    ActionDescription.tryFromLfActionNode(
      ExampleTransactionFactory.createNode(createdId, contractInst),
      Some(ExampleTransactionFactory.lfHash(5)),
      defaultPackagePreference,
      testedProtocolVersion,
    )

  forEvery(factory.standardHappyCases) { example =>
    s"The views of $example" when {

      forEvery(example.viewWithSubviews.zipWithIndex) { case ((view, subviews), index) =>
        s"processing $index-th view" can {
          "be folded" in {
            val foldedSubviews =
              view.foldLeft(Seq.newBuilder[TransactionView])((acc, v) => acc += v)

            foldedSubviews.result() should equal(subviews)
          }

          "be flattened" in {
            view.flatten should equal(subviews)
          }
        }
      }
    }
  }

  "A view" when {
    val firstSubviewIndex = TransactionSubviews.indices(1).head.toString
    "a child view has the same view common data" must {
      val view = factory.SingleCreate(seed = ExampleTransactionFactory.lfHash(3)).view0
      val subViews = TransactionSubviews(Seq(view))(testedProtocolVersion, factory.cryptoOps)
      "reject creation" in {
        TransactionView.create(hashOps)(
          view.viewCommonData,
          view.viewParticipantData,
          subViews,
          testedProtocolVersion,
        ) shouldEqual Left(
          s"The subview with index $firstSubviewIndex has equal viewCommonData to a parent."
        )
      }
    }

    "a child view has package preferences not in the parent" must {

      val unexpectedPackage = LfPackageId.assertFromString("u1")

      val view = factory.SingleExercise(seed = ExampleTransactionFactory.lfHash(3)).view0

      val subview =
        identity[TransactionView]
          .andThen(
            TransactionView.viewParticipantDataUnsafe
              .modify { w =>
                val d = w.tryUnwrap
                val p = d.actionDescription.toProtoV30
                p.getExercise.withPackagePreference(Seq(unexpectedPackage))
                val n = p.withExercise(p.getExercise.withPackagePreference(Seq(unexpectedPackage)))
                d.copy(actionDescription = ActionDescription.fromProtoV30(n).value)
              }
          )
          .andThen(
            // Ensure common data is different from parent
            TransactionView.viewCommonDataUnsafe
              .modify(_.tryUnwrap.copy(salt = TestSalt.generateSalt(23)))
          )
          .apply(view)

      val subViews = TransactionSubviews(Seq(subview))(testedProtocolVersion, factory.cryptoOps)

      "reject creation" in {
        TransactionView
          .create(hashOps)(
            view.viewCommonData,
            view.viewParticipantData,
            subViews,
            testedProtocolVersion,
          )
          .left
          .value shouldBe s"Detected unexpected exercise package preference: $unexpectedPackage at $firstSubviewIndex"
      }
    }
  }

  "A view participant data" when {

    def create(
        actionDescription: ActionDescription = defaultActionDescription,
        consumed: Set[LfContractId] = Set.empty,
        coreInputs: Map[LfContractId, SerializableContract] = Map.empty,
        createdIds: Seq[LfContractId] = Seq(createdId),
        archivedInSubviews: Set[LfContractId] = Set.empty,
        resolvedKeys: Map[LfGlobalKey, LfVersioned[SerializableKeyResolution]] = Map.empty,
    ): Either[String, ViewParticipantData] = {

      val created = createdIds.map { id =>
        val serializable = ExampleTransactionFactory.asSerializable(
          id,
          contractInstance = ExampleTransactionFactory.contractInstance(),
          metadata = ContractMetadata.empty,
          salt = TestSalt.generateSalt(1),
        )
        CreatedContract.tryCreate(serializable, consumed.contains(id), rolledBack = false)
      }
      val coreInputs2 = coreInputs.transform { (id, contract) =>
        InputContract(contract, consumed.contains(id))
      }

      ViewParticipantData
        .create(hashOps)(
          coreInputs2,
          created,
          archivedInSubviews,
          resolvedKeys,
          actionDescription,
          RollbackContext.empty,
          salt,
          testedProtocolVersion,
        )
        .flatMap { data =>
          // Return error message if root action is not valid
          Either
            .catchOnly[InvalidViewParticipantData](data.rootAction)
            .bimap(ex => ex.message, _ => data)
        }
    }

    "a contract is created twice" must {
      "reject creation" in {
        create(createdIds = Seq(createdId, createdId)).left.value should
          startWith regex "createdCore contains the contract id .* multiple times at indices 0, 1"
      }
    }
    "a used contract has an inconsistent id" must {
      "reject creation" in {
        val usedContract =
          ExampleTransactionFactory.asSerializable(
            otherAbsoluteId,
            metadata = ContractMetadata.empty,
          )

        create(coreInputs = Map(absoluteId -> usedContract)).left.value should startWith(
          "Inconsistent ids for used contract: "
        )
      }
    }
    "an overlap between archivedInSubview and coreCreated" must {
      "reject creation" in {
        create(
          createdIds = Seq(createdId),
          archivedInSubviews = Set(createdId),
        ).left.value should startWith(
          "Contract created in a subview are also created in the core: "
        )
      }
    }
    "an overlap between archivedInSubview and coreInputs" must {
      "reject creation" in {
        val usedContract =
          ExampleTransactionFactory.asSerializable(absoluteId, metadata = ContractMetadata.empty)

        create(
          coreInputs = Map(absoluteId -> usedContract),
          archivedInSubviews = Set(absoluteId),
        ).left.value should startWith("Contracts created in a subview overlap with core inputs: ")
      }
    }
    "the created contract of the root action is not declared first" must {
      "reject creation" in {
        create(createdIds = Seq.empty).left.value should startWith(
          "No created core contracts declared for a view that creates contract"
        )
      }
      "reject creation with other contract ids" in {
        val otherCantonId =
          cantonContractIdVersion.fromDiscriminator(
            ExampleTransactionFactory.lfHash(3),
            ExampleTransactionFactory.unicum(1),
          )
        create(createdIds = Seq(otherCantonId, createdId)).left.value should startWith(
          show"View with root action Create $createdId declares $otherCantonId as first created core contract."
        )
      }
    }
    "the used contract of the root action is not declared" must {

      "reject creation with exercise action" in {
        create(
          actionDescription = ActionDescription.tryFromLfActionNode(
            ExampleTransactionFactory.exerciseNodeWithoutChildren(absoluteId),
            Some(nodeSeed),
            defaultPackagePreference,
            testedProtocolVersion,
          )
        ).left.value should startWith(
          show"Input contract $absoluteId of the Exercise root action is not declared as core input."
        )
      }

      "reject creation with fetch action" in {

        create(
          actionDescription = ActionDescription.tryFromLfActionNode(
            ExampleTransactionFactory.fetchNode(
              absoluteId,
              Set(ExampleTransactionFactory.submitter),
            ),
            None,
            defaultPackagePreference,
            testedProtocolVersion,
          )
        ).left.value should startWith(
          show"Input contract $absoluteId of the Fetch root action is not declared as core input."
        )
      }

      "reject creation with lookup action" in {
        create(
          actionDescription = ActionDescription.tryFromLfActionNode(
            ExampleTransactionFactory.lookupByKeyNode(
              globalKey,
              maintainers = Set(ExampleTransactionFactory.submitter),
            ),
            None,
            defaultPackagePreference,
            testedProtocolVersion,
          )
        ).left.value should startWith(
          show"Key $globalKey of LookupByKey root action is not resolved."
        )

      }
    }

    "deserialized" must {
      "reconstruct the original view participant data" in {
        val usedContract =
          ExampleTransactionFactory.asSerializable(
            absoluteId,
            metadata = ContractMetadata.tryCreate(
              Set.empty,
              Set.empty,
              Some(ExampleTransactionFactory.globalKeyWithMaintainers()),
            ),
          )

        val vpd = create(
          consumed = Set(absoluteId),
          createdIds = Seq(createdId),
          coreInputs = Map(absoluteId -> usedContract),
          archivedInSubviews = Set(otherAbsoluteId),
          resolvedKeys = Map(
            ExampleTransactionFactory.defaultGlobalKey ->
              LfVersioned(ExampleTransactionFactory.transactionVersion, AssignedKey(absoluteId))
          ),
        ).value

        ViewParticipantData
          .fromByteString(testedProtocolVersion)(hashOps)(
            vpd.getCryptographicEvidence
          )
          .map(_.unwrap) shouldBe Right(Right(vpd))
      }
    }
  }
}
