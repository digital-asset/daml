// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.platform.partymanagement

import com.daml.ledger.api.domain.{ObjectMeta, ParticipantParty}
import com.daml.ledger.participant.state.index.v2.AnnotationsUpdate.{Merge, Replace}
import com.daml.ledger.participant.state.index.v2.PartyRecordStore.PartyRecordExistsFatal
import com.daml.ledger.participant.state.index.v2.{
  AnnotationsUpdate,
  ObjectMetaUpdate,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future
import scala.language.implicitConversions

trait PartyRecordStoreTests extends PartyRecordStoreSpecBase { self: AsyncFreeSpec =>

  implicit val lc: LoggingContext = LoggingContext.ForTesting

  private implicit def toParty(s: String): Party =
    Party.assertFromString(s)

  private val party1 = "party1"

  def newPartyRecord(
      name: String = party1,
      annotations: Map[String, String] = Map.empty,
  ): ParticipantParty.PartyRecord =
    ParticipantParty.PartyRecord(
      party = name,
      metadata = ObjectMeta(None, annotations = annotations),
    )

  def createdPartyRecord(
      name: String = party1,
      annotations: Map[String, String] = Map.empty,
      resourceVersion: Long = 0,
  ): ParticipantParty.PartyRecord =
    ParticipantParty.PartyRecord(
      party = name,
      metadata = ObjectMeta(
        resourceVersionO = Some(resourceVersion),
        annotations = annotations,
      ),
    )

  def makePartRecordUpdate(
      party: Ref.Party = party1,
      annotationsUpdateO: Option[AnnotationsUpdate] = None,
  ): PartyRecordUpdate = PartyRecordUpdate(
    party = party,
    metadataUpdate = ObjectMetaUpdate(
      resourceVersionO = None,
      annotationsUpdateO = annotationsUpdateO,
    ),
  )

  def resetResourceVersion(
      partyRecord: ParticipantParty.PartyRecord
  ): ParticipantParty.PartyRecord =
    partyRecord.copy(metadata = partyRecord.metadata.copy(resourceVersionO = None))

  "party record store" - {

    "creating" - {
      "allow creating a fresh party record" in {
        testIt { tested =>
          for {
            create1 <- tested.createPartyRecord(newPartyRecord("party1"))
            create2 <- tested.createPartyRecord(newPartyRecord("party2"))
          } yield {
            create1.value shouldBe createdPartyRecord("party1")
            create2.value shouldBe createdPartyRecord("party2")
          }
        }
      }

      "disallow re-creating an existing party record" in {
        testIt { tested =>
          for {
            create1 <- tested.createPartyRecord(newPartyRecord("party1"))
            create1b <- tested.createPartyRecord(newPartyRecord("party1"))
          } yield {
            create1.value shouldBe createdPartyRecord("party1")
            create1b.left.value shouldBe PartyRecordExistsFatal(create1.value.party)
          }
        }
      }
    }

    "getting" - {
      "find a freshly created party record" in {
        testIt { tested =>
          val newPr = newPartyRecord("party1")
          for {
            create1 <- tested.createPartyRecord(newPr)
            get1 <- tested.getPartyRecordO(newPr.party)
          } yield {
            create1.value shouldBe createdPartyRecord("party1")
            get1.value shouldBe Some(createdPartyRecord("party1"))
          }
        }
      }

      "return None for a non-existent party record" in {
        testIt { tested =>
          val party = Ref.Party.assertFromString("party1")
          for {
            get1 <- tested.getPartyRecordO(party)
          } yield {
            get1.value shouldBe None
          }
        }
      }
    }

    "updating" - {
      "update an existing party record" in {
        testIt { tested =>
          val pr1 = newPartyRecord("party1")
          for {
            create1 <- tested.createPartyRecord(pr1)
            _ = create1.value shouldBe createdPartyRecord("party1")
            update1 <- tested.updatePartyRecord(
              partyRecordUpdate = PartyRecordUpdate(
                party = pr1.party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = create1.value.metadata.resourceVersionO,
                  annotationsUpdateO = Some(Merge.fromNonEmpty(Map("k1" -> "v1"))),
                ),
              ),
              ledgerPartyExists = _ => Future.successful(true),
            )
            _ = resetResourceVersion(update1.value) shouldBe newPartyRecord(
              "party1",
              annotations = Map("k1" -> "v1"),
            )
          } yield succeed
        }
      }
      "should succeed when updating a non-existing party record for which a ledger party exists" in {
        testIt { tested =>
          val party = Ref.Party.assertFromString("party1")
          for {
            update1 <- tested.updatePartyRecord(
              partyRecordUpdate = PartyRecordUpdate(
                party = party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = None,
                  annotationsUpdateO = Some(
                    Merge.fromNonEmpty(
                      Map(
                        "k1" -> "v1",
                        "k2" -> "v2",
                      )
                    )
                  ),
                ),
              ),
              ledgerPartyExists = _ => Future.successful(true),
            )
            _ = update1.value shouldBe createdPartyRecord(
              "party1",
              resourceVersion = 0,
              annotations = Map("k1" -> "v1", "k2" -> "v2"),
            )
          } yield succeed
        }
      }

      "should update metadata annotations with both merge and replace semantics" in {
        testIt { tested =>
          val pr = createdPartyRecord("party1", annotations = Map("k1" -> "v1", "k2" -> "v2"))
          for {
            create1 <- tested.createPartyRecord(
              partyRecord = pr
            )
            _ = create1.value shouldBe createdPartyRecord(
              "party1",
              resourceVersion = 0,
              annotations = Map("k1" -> "v1", "k2" -> "v2"),
            )
            // first update: with merge annotations semantics
            update1 <- tested.updatePartyRecord(
              partyRecordUpdate = PartyRecordUpdate(
                party = pr.party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = None,
                  annotationsUpdateO = Some(
                    Merge.fromNonEmpty(
                      Map(
                        "k1" -> "v1b",
                        "k3" -> "v3",
                      )
                    )
                  ),
                ),
              ),
              ledgerPartyExists = _ => Future.successful(true),
            )
            _ = update1.value shouldBe createdPartyRecord(
              "party1",
              resourceVersion = 1,
              annotations = Map("k1" -> "v1b", "k2" -> "v2", "k3" -> "v3"),
            )
            // second update: with replace annotations semantics
            update2 <- tested.updatePartyRecord(
              partyRecordUpdate = PartyRecordUpdate(
                party = pr.party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = None,
                  annotationsUpdateO = Some(
                    Replace(
                      Map(
                        "k1" -> "v1c",
                        "k4" -> "v4",
                      )
                    )
                  ),
                ),
              ),
              ledgerPartyExists = _ => Future.successful(true),
            )
            _ = update2.value shouldBe createdPartyRecord(
              "party1",
              resourceVersion = 2,
              annotations = Map("k1" -> "v1c", "k4" -> "v4"),
            )
            // third update: with replace annotations semantics - effectively deleting all annotations
            update3 <- tested.updatePartyRecord(
              partyRecordUpdate = PartyRecordUpdate(
                party = pr.party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = None,
                  annotationsUpdateO = Some(Replace(Map.empty)),
                ),
              ),
              ledgerPartyExists = _ => Future.successful(true),
            )
            _ = update3.value shouldBe createdPartyRecord(
              "party1",
              resourceVersion = 3,
              annotations = Map.empty,
            )
          } yield {
            succeed
          }
        }
      }

      "should raise an error when updating a non-existing party record for which a ledger party doesn't exist" in {
        testIt { tested =>
          val party = Ref.Party.assertFromString("party")
          for {
            res1 <- tested.updatePartyRecord(
              partyRecordUpdate = PartyRecordUpdate(
                party = party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = None,
                  annotationsUpdateO = Some(Merge.fromNonEmpty(Map("k1" -> "v1"))),
                ),
              ),
              ledgerPartyExists = _ => Future.successful(false),
            )
            _ = res1.left.value shouldBe PartyRecordStore.PartyNotFound(party)
          } yield succeed
        }
      }

      "should raise an error on resource version mismatch" in {
        testIt { tested =>
          val pr = createdPartyRecord("party1")
          for {
            _ <- tested.createPartyRecord(pr)
            res1 <- tested.updatePartyRecord(
              PartyRecordUpdate(
                party = pr.party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = Some(100),
                  annotationsUpdateO = Some(Merge.fromNonEmpty(Map("k1" -> "v1"))),
                ),
              ),
              ledgerPartyExists = _ => Future.successful(true),
            )
            _ = res1.left.value shouldBe PartyRecordStore.ConcurrentPartyUpdate(pr.party)
          } yield succeed
        }
      }
    }

    "raise an error when annotations byte size max size exceeded" - {
      // This value consumes just a bit over half the allowed max size limit
      val bigValue = "big value:" + ("a" * 128 * 1024)

      "when creating a party record" in {
        testIt { tested =>
          val pr = newPartyRecord("party1", annotations = Map("k1" -> bigValue, "k2" -> bigValue))
          for {
            res1 <- tested.createPartyRecord(pr)
            _ = res1.left.value shouldBe PartyRecordStore.MaxAnnotationsSizeExceeded(pr.party)
          } yield succeed
        }
      }

      "when updating an existing party record" in {
        testIt { tested =>
          val pr = newPartyRecord("party1", annotations = Map("k1" -> bigValue))
          for {
            _ <- tested.createPartyRecord(pr)
            res1 <- tested.updatePartyRecord(
              makePartRecordUpdate(annotationsUpdateO =
                Some(Merge.fromNonEmpty(Map("k2" -> bigValue)))
              ),
              ledgerPartyExists = (_ => Future.successful(true)),
            )
            _ = res1.left.value shouldBe PartyRecordStore.MaxAnnotationsSizeExceeded(pr.party)
          } yield succeed
        }
      }

      "when updating non-existent party record" in {
        testIt { tested =>
          val party = Ref.Party.assertFromString("party1")
          for {
            res1 <- tested.updatePartyRecord(
              makePartRecordUpdate(annotationsUpdateO =
                Some(Merge.fromNonEmpty(Map("k1" -> bigValue, "k2" -> bigValue)))
              ),
              ledgerPartyExists = (_ => Future.successful(true)),
            )
            _ = res1.left.value shouldBe PartyRecordStore.MaxAnnotationsSizeExceeded(party)
          } yield succeed
        }
      }
    }

  }

}
