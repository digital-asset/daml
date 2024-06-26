// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderConfig,
  IdentityProviderId,
  JwksUrl,
  ObjectMeta,
}
import com.digitalasset.canton.ledger.localstore.api.PartyRecordStore.{
  PartyNotFound,
  PartyRecordExistsFatal,
}
import com.digitalasset.canton.ledger.localstore.api.{
  ObjectMetaUpdate,
  PartyRecord,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{LedgerString, Party}
import org.scalatest.freespec.AsyncFreeSpec

import scala.language.implicitConversions

trait PartyRecordStoreTests extends PartyRecordStoreSpecBase { self: AsyncFreeSpec =>

  implicit val lc: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private implicit def toParty(s: String): Party =
    Party.assertFromString(s)

  private val party1 = "party1"
  private val defaultIdpId = IdentityProviderId.Default
  private val idpId1 = IdentityProviderId.Id(LedgerString.assertFromString("idp1"))
  private val idpId2 = IdentityProviderId.Id(LedgerString.assertFromString("idp2"))
  private val idp1 = IdentityProviderConfig(
    identityProviderId = idpId1,
    isDeactivated = false,
    jwksUrl = JwksUrl("http://domain.com/"),
    issuer = "issuer",
    audience = Some("audience"),
  )
  private val idp2 = IdentityProviderConfig(
    identityProviderId = idpId2,
    isDeactivated = false,
    jwksUrl = JwksUrl("http://domain2.com/"),
    issuer = "issuer2",
    audience = Some("audience"),
  )

  def newPartyRecord(
      name: String = party1,
      annotations: Map[String, String] = Map.empty,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): PartyRecord =
    PartyRecord(
      party = name,
      metadata = ObjectMeta(None, annotations = annotations),
      identityProviderId = identityProviderId,
    )

  def createdPartyRecord(
      name: String = party1,
      annotations: Map[String, String] = Map.empty,
      resourceVersion: Long = 0,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): PartyRecord =
    PartyRecord(
      party = name,
      metadata = ObjectMeta(
        resourceVersionO = Some(resourceVersion),
        annotations = annotations,
      ),
      identityProviderId = identityProviderId,
    )

  def makePartRecordUpdate(
      party: Ref.Party = party1,
      annotationsUpdateO: Option[Map[String, String]] = None,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): PartyRecordUpdate = PartyRecordUpdate(
    party = party,
    metadataUpdate = ObjectMetaUpdate(
      resourceVersionO = None,
      annotationsUpdateO = annotationsUpdateO,
    ),
    identityProviderId = identityProviderId,
  )

  def resetResourceVersion(
      partyRecord: PartyRecord
  ): PartyRecord =
    partyRecord.copy(metadata = partyRecord.metadata.copy(resourceVersionO = None))

  "party record store" - {

    "creating" - {
      "allow creating a fresh party record" in {
        testIt { tested =>
          for {
            _ <- createIdentityProviderConfig(idp1)
            create1 <- tested.createPartyRecord(newPartyRecord("party1"))
            create2 <- tested.createPartyRecord(newPartyRecord("party2"))
            create3 <- tested.createPartyRecord(
              newPartyRecord("party3", identityProviderId = idpId1)
            )
          } yield {
            create1.value shouldBe createdPartyRecord("party1")
            create2.value shouldBe createdPartyRecord("party2")
            create3.value shouldBe createdPartyRecord(
              "party3",
              identityProviderId = idpId1,
            )
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
                  annotationsUpdateO = Some(Map("k1" -> "v1")),
                ),
                identityProviderId = IdentityProviderId.Default,
              ),
              ledgerPartyIsLocal = true,
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
            _ <- createIdentityProviderConfig(idp1)
            update1 <- tested.updatePartyRecord(
              partyRecordUpdate = PartyRecordUpdate(
                party = party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = None,
                  annotationsUpdateO = Some(
                    Map(
                      "k1" -> "v1",
                      "k2" -> "v2",
                    )
                  ),
                ),
                identityProviderId = idpId1,
              ),
              ledgerPartyIsLocal = true,
            )
            _ = update1.value shouldBe createdPartyRecord(
              "party1",
              resourceVersion = 0,
              annotations = Map("k1" -> "v1", "k2" -> "v2"),
              identityProviderId = idpId1,
            )
          } yield succeed
        }
      }

      "should add, update and remove annotations" in {
        testIt { tested =>
          val pr = createdPartyRecord(
            "party1",
            annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"),
          )
          for {
            _ <- tested.createPartyRecord(
              partyRecord = pr
            )
            // first update: with merge annotations semantics
            update1 <- tested.updatePartyRecord(
              partyRecordUpdate = PartyRecordUpdate(
                party = pr.party,
                metadataUpdate = ObjectMetaUpdate(
                  resourceVersionO = None,
                  annotationsUpdateO = Some(
                    Map(
                      // updating
                      "k1" -> "v1b",
                      // deleting
                      "k3" -> "",
                      // adding
                      "k4" -> "v4",
                    )
                  ),
                ),
                identityProviderId = IdentityProviderId.Default,
              ),
              ledgerPartyIsLocal = true,
            )
            _ = update1.value shouldBe createdPartyRecord(
              "party1",
              resourceVersion = 1,
              annotations = Map("k1" -> "v1b", "k2" -> "v2", "k4" -> "v4"),
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
                  annotationsUpdateO = Some(Map("k1" -> "v1")),
                ),
                identityProviderId = IdentityProviderId.Default,
              ),
              ledgerPartyIsLocal = false,
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
                  annotationsUpdateO = Some(Map("k1" -> "v1")),
                ),
                identityProviderId = IdentityProviderId.Default,
              ),
              ledgerPartyIsLocal = true,
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
              makePartRecordUpdate(annotationsUpdateO = Some(Map("k2" -> bigValue))),
              ledgerPartyIsLocal = true,
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
                Some(Map("k1" -> bigValue, "k2" -> bigValue))
              ),
              ledgerPartyIsLocal = true,
            )
            _ = res1.left.value shouldBe PartyRecordStore.MaxAnnotationsSizeExceeded(party)
          } yield succeed
        }
      }
    }

  }

  "reassigning idp" - {
    "change party's idp" in {
      testIt { tested =>
        for {
          create <- tested.createPartyRecord(
            newPartyRecord("p1", identityProviderId = defaultIdpId)
          )
          _ = create.value shouldBe createdPartyRecord("p1", identityProviderId = defaultIdpId)
          _ <- createIdentityProviderConfig(idp1)
          updated <- tested.updatePartyRecordIdp(
            sourceIdp = defaultIdpId,
            targetIdp = idpId1,
            party = create.value.party,
            ledgerPartyIsLocal = true,
          )
          _ <- updated.value.identityProviderId shouldBe idpId1
        } yield succeed
      }
    }

    "when using wrong source idp id" in {
      testIt { tested =>
        for {
          _ <- createIdentityProviderConfig(idp1)
          _ <- createIdentityProviderConfig(idp2)
          create <- tested.createPartyRecord(newPartyRecord("p1", identityProviderId = idpId1))
          _ = create.value shouldBe createdPartyRecord("p1", identityProviderId = idpId1)
          updateResult <- tested.updatePartyRecordIdp(
            sourceIdp = idpId2,
            targetIdp = defaultIdpId,
            party = create.value.party,
            ledgerPartyIsLocal = true,
          )
          _ <- updateResult.left.value shouldBe PartyNotFound(create.value.party)
        } yield succeed
      }
    }

    "cannot change idp for non-existent party-record for non-local party" in {
      testIt { tested =>
        val party = Ref.Party.assertFromString("party")
        for {
          _ <- createIdentityProviderConfig(idp1)
          updated <- tested.updatePartyRecordIdp(
            sourceIdp = defaultIdpId,
            targetIdp = idpId1,
            party = party,
            ledgerPartyIsLocal = false,
          )
          _ <- updated.left.value shouldBe PartyNotFound(party)
        } yield succeed
      }
    }

    "can change idp for non-existent party-record for local party" in {
      testIt { tested =>
        val party = Ref.Party.assertFromString("party")
        for {
          _ <- createIdentityProviderConfig(idp1)
          updated <- tested.updatePartyRecordIdp(
            sourceIdp = defaultIdpId,
            targetIdp = idpId1,
            party = party,
            ledgerPartyIsLocal = true,
          )
          _ <- updated.value.identityProviderId shouldBe idpId1
        } yield succeed
      }
    }

  }

}
