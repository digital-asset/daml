// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.value.Value
import com.daml.script.export.TreeUtils.SubmitSimpleMulti
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeSubmitSpec extends AnyFreeSpec with Matchers {
  import Encode._
  "encodeSubmit" - {
    "multi-party submissions" in {
      val parties = Map(
        Party("Alice") -> "alice_0",
        Party("Bob") -> "bob_0",
      )
      val cidMap = Map(
        ContractId("cid1") -> "contract_0_0",
        ContractId("cid2") -> "contract_0_1",
      )
      val cidRefs = Set.empty[ContractId]
      val submit = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1"), submitters = Seq(Party("Alice"))),
            TestData.Exercised(ContractId("cid2"), Seq.empty, actingParties = Seq(Party("Bob"))),
          )
        )
        .toSubmit
      encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
        """tree <- submitTreeMulti [alice_0, bob_0] [] do
          |  createCmd Module.Template
          |  exerciseCmd contract_0_1 (Module.Choice ())""".stripMargin.replace("\r\n", "\n")
    }
    "contract id bindings" - {
      "unreferenced create" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set.empty[ContractId]
        val submit = TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("cid1"))
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
          """_ <- submit alice_0 do
            |  createCmd Module.Template""".stripMargin.replace("\r\n", "\n")
      }
      "unreferenced creates" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid2") -> "contract_1_1",
        )
        val cidRefs = Set.empty[ContractId]
        val submit = TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("cid1")),
              TestData.Created(ContractId("cid2")),
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
          """submit alice_0 do
            |  _ <- createCmd Module.Template
            |  _ <- createCmd Module.Template
            |  pure ()""".stripMargin.replace("\r\n", "\n")
      }
      "unreferenced exercise" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid0") -> "contract_0_0",
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid1") -> "contract_1_1",
        )
        val cidRefs = Set.empty[ContractId]
        val submit = TestData
          .Tree(
            Seq(
              TestData.Exercised(
                ContractId("cid0"),
                Seq(
                  TestData.Created(ContractId("cid1"))
                ),
              )
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  exerciseCmd contract_0_0 (Module.Choice ())""".stripMargin.replace("\r\n", "\n")
      }
      "referenced create" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(ContractId("cid1") -> "contract_0_0")
        val cidRefs = Set(ContractId("cid1"))
        val submit = TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("cid1"))
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
          """(coerceContractId @_ @Module.Template -> contract_0_0) <- submit alice_0 do
            |  createCmd Module.Template""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "referenced creates" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid2") -> "contract_1_1",
        )
        val cidRefs = Set(ContractId("cid1"), ContractId("cid2"))
        val submit = TestData
          .Tree(
            Seq(
              TestData.Created(ContractId("cid1")),
              TestData.Created(ContractId("cid2")),
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
          """((coerceContractId @_ @Module.Template -> contract_1_0),
            |    (coerceContractId @_ @Module.Template -> contract_1_1)) <- submit alice_0 do
            |  (coerceContractId @_ @Module.Template -> contract_1_0) <- createCmd Module.Template
            |  (coerceContractId @_ @Module.Template -> contract_1_1) <- createCmd Module.Template
            |  pure (contract_1_0, contract_1_1)""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "referenced exercise" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid0") -> "contract_0_0",
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid2") -> "contract_1_1",
          ContractId("cid3") -> "contract_0_1",
          ContractId("cid4") -> "contract_2_0",
          ContractId("cid5") -> "contract_2_1",
        )
        val cidRefs = Set(ContractId("cid1"), ContractId("cid2"), ContractId("cid4"))
        val submit = TestData
          .Tree(
            Seq(
              TestData.Exercised(
                ContractId("cid0"),
                Seq(
                  TestData.Created(ContractId("cid1")),
                  TestData.Created(ContractId("cid2")),
                ),
              ),
              TestData.Exercised(
                ContractId("cid3"),
                Seq(
                  TestData.Created(ContractId("cid4")),
                  TestData.Created(ContractId("cid5")),
                ),
              ),
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  exerciseCmd contract_0_0 (Module.Choice ())
            |  exerciseCmd contract_0_1 (Module.Choice ())
            |let (coerceContractId @_ @Module.Template -> contract_1_0) = fromTree tree $
            |      exercised @Module.Template "Choice" $
            |      created @Module.Template
            |let (coerceContractId @_ @Module.Template -> contract_1_1) = fromTree tree $
            |      exercised @Module.Template "Choice" $
            |      createdN @Module.Template 1
            |let (coerceContractId @_ @Module.Template -> contract_2_0) = fromTree tree $
            |      exercisedN @Module.Template "Choice" 1 $
            |      created @Module.Template""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "referenced exerciseByKey" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid0") -> "contract_0_0",
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid2") -> "contract_1_1",
        )
        val cidRefs = Set(ContractId("cid1"), ContractId("cid2"))
        val submit = TestData
          .Tree(
            Seq[TestData.Event](
              TestData.Created(ContractId("cid0"), contractKey = Some(Value().withParty("Alice"))),
              TestData.Created(ContractId("cid1")),
              TestData.Exercised(
                ContractId("cid0"),
                Seq(
                  TestData.Created(ContractId("cid2"))
                ),
              ),
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  createCmd Module.Template
            |  createCmd Module.Template
            |  exerciseByKeyCmd @Module.Template alice_0 (Module.Choice ())
            |let (coerceContractId @_ @Module.Template -> contract_1_0) = fromTree tree $
            |      createdN @Module.Template 1
            |let (coerceContractId @_ @Module.Template -> contract_1_1) = fromTree tree $
            |      exercised @Module.Template "Choice" $
            |      created @Module.Template""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "unreferenced createAndExercise" in {
        val parties = Map(Party("Alice") -> "alice_0")
        val cidMap = Map(
          ContractId("cid0") -> "contract_0_0",
          ContractId("cid1") -> "contract_1_0",
          ContractId("cid1") -> "contract_1_1",
        )
        val cidRefs = Set.empty[ContractId]
        val submit = TestData
          .Tree(
            Seq[TestData.Event](
              TestData.Created(ContractId("cid0")),
              TestData.Exercised(
                ContractId("cid0"),
                Seq(
                  TestData.Created(ContractId("cid1"))
                ),
              ),
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, Set.empty, submit).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  createAndExerciseCmd
            |    Module.Template
            |    (Module.Choice ())""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "referenced mixed simple create and exercise commands" in {
        val parties = Map(
          Party("Alice") -> "alice_0",
          Party("Bob") -> "bob_0",
        )
        val cidMap = Map(
          ContractId("cid1") -> "contract_0_0",
          ContractId("cid2") -> "contract_0_1",
          ContractId("cid3") -> "contract_0_2",
          ContractId("cid4") -> "contract_0_3",
          ContractId("cid5") -> "contract_0_4",
        )
        val cidRefs = Set(ContractId("cid1"), ContractId("cid3"), ContractId("cid5"))
        val commands = TestData
          .Tree(
            Seq[TestData.Event](
              TestData.Created(ContractId("cid1"), submitters = Seq(Party("Alice"))),
              TestData.Exercised(
                ContractId("cid2"),
                Seq(
                  TestData.Created(ContractId("cid3"))
                ),
                exerciseResult = Some(ContractId("cid3")),
                actingParties = Seq(Party("Bob")),
              ),
              TestData.Created(ContractId("cid4"), submitters = Seq(Party("Alice"))),
              TestData.Exercised(
                ContractId("cid4"),
                Seq(
                  TestData.Created(ContractId("cid5"))
                ),
                exerciseResult = Some(ContractId("cid5")),
                actingParties = Seq(Party("Bob")),
              ),
            )
          )
          .toSimpleCommands
        encodeSubmit(
          parties,
          cidMap,
          cidRefs,
          Set.empty,
          SubmitSimpleMulti(commands, parties.keySet),
        )
          .render(80) shouldBe
          """((coerceContractId @_ @Module.Template -> contract_0_0),
            |    (coerceContractId @_ @Module.Template -> contract_0_2),
            |    (coerceContractId @_ @Module.Template -> contract_0_4)) <- submitMulti [alice_0, bob_0] [] do
            |  (coerceContractId @_ @Module.Template -> contract_0_0) <- createCmd Module.Template
            |  (coerceContractId @_ @Module.Template -> contract_0_2) <- exerciseCmd contract_0_1 (Module.Choice ())
            |  (coerceContractId @_ @Module.Template -> contract_0_4) <- createAndExerciseCmd
            |    Module.Template
            |    (Module.Choice ())
            |  pure (contract_0_0, contract_0_2, contract_0_4)""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
    }
    "handling contracts missing instances" - {
      "create" in {
        val parties = Map(
          Party("Alice") -> "alice_0"
        )
        val cidMap = Map.empty[ContractId, String]
        val cidRefs = Set.empty[ContractId]
        val missingInstances = Set(ApiTypes.TemplateId(TestData.defaultTemplateId))
        val submit = TestData
          .Tree(
            Seq[TestData.Event](
              TestData.Created(ContractId("cid1"), submitters = Seq(Party("Alice")))
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, missingInstances, submit).render(80) shouldBe
          """_ <- submit alice_0 do
            |  internalCreateCmd (toAnyTemplate Module.Template)""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "exercise" in {
        val parties = Map(
          Party("Alice") -> "alice_0"
        )
        val cidMap = Map(
          ContractId("cid1") -> "contract_0_0"
        )
        val cidRefs = Set.empty[ContractId]
        val missingInstances = Set(ApiTypes.TemplateId(TestData.defaultTemplateId))
        val submit = TestData
          .Tree(
            Seq[TestData.Event](
              TestData.Exercised(
                ContractId("cid1"),
                Seq.empty[TestData.Event],
                actingParties = Seq(Party("Alice")),
              )
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, missingInstances, submit).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  internalExerciseCmd
            |    (templateTypeRep @Module.Template)
            |    (coerceContractId contract_0_0)
            |    (toAnyChoice @Module.Template (Module.Choice ()))""".stripMargin
            .replace("\r\n", "\n")
      }
      "createAndExercise" in {
        val parties = Map(
          Party("Alice") -> "alice_0"
        )
        val cidMap = Map(
          ContractId("cid1") -> "contract_0_0"
        )
        val cidRefs = Set.empty[ContractId]
        val missingInstances = Set(ApiTypes.TemplateId(TestData.defaultTemplateId))
        val submit = TestData
          .Tree(
            Seq[TestData.Event](
              TestData.Created(ContractId("cid1"), submitters = Seq(Party("Alice"))),
              TestData.Exercised(
                ContractId("cid1"),
                Seq.empty[TestData.Event],
                actingParties = Seq(Party("Alice")),
              ),
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, missingInstances, submit).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  internalCreateAndExerciseCmd
            |    (toAnyTemplate Module.Template)
            |    (toAnyChoice @Module.Template (Module.Choice ()))""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
      "exerciseByKey" in {
        val parties = Map(
          Party("Alice") -> "alice_0"
        )
        val cidMap = Map(
          ContractId("cid1") -> "contract_0_0",
          ContractId("cid2") -> "contract_0_1",
        )
        val cidRefs = Set.empty[ContractId]
        val missingInstances = Set(ApiTypes.TemplateId(TestData.defaultTemplateId))
        val submit = TestData
          .Tree(
            Seq[TestData.Event](
              TestData.Exercised(
                ContractId("cid1"),
                Seq[TestData.Event](
                  TestData.Created(
                    ContractId("cid2"),
                    submitters = Seq(Party("Alice")),
                    contractKey = Some(Value().withParty("Alice")),
                  )
                ),
                actingParties = Seq(Party("Alice")),
              ),
              TestData.Exercised(
                ContractId("cid2"),
                Seq.empty[TestData.Event],
                actingParties = Seq(Party("Alice")),
              ),
            )
          )
          .toSubmit
        encodeSubmit(parties, cidMap, cidRefs, missingInstances, submit).render(80) shouldBe
          """tree <- submitTree alice_0 do
            |  internalExerciseCmd
            |    (templateTypeRep @Module.Template)
            |    (coerceContractId contract_0_0)
            |    (toAnyChoice @Module.Template (Module.Choice ()))
            |  internalExerciseByKeyCmd
            |    (templateTypeRep @Module.Template)
            |    (toAnyContractKey @Module.Template alice_0)
            |    (toAnyChoice @Module.Template (Module.Choice ()))""".stripMargin.replace(
            "\r\n",
            "\n",
          )
      }
    }
  }
}
