// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore

import com.daml.lf.data.Ref
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderConfig,
  IdentityProviderId,
  JwksUrl,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.localstore.api.IdentityProviderConfigStore.{
  IdentityProviderConfigByIssuerNotFound,
  IdentityProviderConfigExists,
  IdentityProviderConfigNotFound,
  IdentityProviderConfigWithIssuerExists,
  TooManyIdentityProviderConfigs,
}
import com.digitalasset.canton.platform.localstore.api.IdentityProviderConfigUpdate
import org.scalatest.freespec.AsyncFreeSpec

import java.util.UUID
import scala.concurrent.Future

trait IdentityProviderConfigStoreTests extends IdentityProviderConfigStoreSpecBase with BaseTest {
  self: AsyncFreeSpec =>
  implicit val lc: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  val MaxIdentityProviderConfigs = 10

  def config(): IdentityProviderConfig =
    IdentityProviderConfig(
      identityProviderId = randomId(),
      isDeactivated = false,
      jwksUrl = JwksUrl.assertFromString("http://example.com/jwks.json"),
      issuer = UUID.randomUUID().toString,
      audience = Some(UUID.randomUUID().toString),
    )

  def randomId() = {
    val id = UUID.randomUUID().toString
    IdentityProviderId.Id(Ref.LedgerString.assertFromString(id))
  }

  "identity provider config store" - {
    "allows to create and load unchanged an identity provider config" in {
      testIt { tested =>
        val cfg1 = config()
        for {
          res1 <- tested.createIdentityProviderConfig(cfg1)
        } yield {
          res1 shouldBe Right(cfg1)
        }
      }
    }

    "disallow to create identity provider config with non unique id" in {
      val id = randomId()
      testIt { tested =>
        val cfg1 = config().copy(identityProviderId = id)
        val cfg2 = config().copy(identityProviderId = id)
        for {
          res1 <- tested.createIdentityProviderConfig(cfg1)
          res2 <- tested.createIdentityProviderConfig(cfg2)
        } yield {
          res1 shouldBe Right(cfg1)
          res2 shouldBe Left(IdentityProviderConfigExists(id))
        }
      }
    }

    "disallow to create identity provider config with non unique issuer" in {
      testIt { tested =>
        val cfg1 = config().copy(issuer = "issuer1")
        val cfg2 = config().copy(issuer = "issuer1")
        for {
          res1 <- tested.createIdentityProviderConfig(cfg1)
          res2 <- tested.createIdentityProviderConfig(cfg2)
        } yield {
          res1 shouldBe Right(cfg1)
          res2 shouldBe Left(IdentityProviderConfigWithIssuerExists("issuer1"))
        }
      }
    }

    s"disallow to create more than $MaxIdentityProviderConfigs configs" in {
      testIt { tested =>
        for {
          res1 <- Future.sequence(
            (1 to MaxIdentityProviderConfigs).map(_ =>
              tested.createIdentityProviderConfig(config())
            )
          )
          last = config()
          res2 <- tested.createIdentityProviderConfig(last)
          res3 <- tested.getIdentityProviderConfig(last.identityProviderId)
        } yield {
          res1.forall(_.isRight) shouldBe true
          res2 shouldBe Left(TooManyIdentityProviderConfigs())
          // check res3 has not been created
          res3 shouldBe Left(IdentityProviderConfigNotFound(last.identityProviderId))
        }
      }
    }

    "allows to delete an identity provider config" in {
      testIt { tested =>
        val cfg = config()
        for {
          res1 <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.deleteIdentityProviderConfig(cfg.identityProviderId)
          res3 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
        } yield {
          res1 shouldBe Right(cfg)
          res2 shouldBe Right(())
          res3 shouldBe Left(IdentityProviderConfigNotFound(cfg.identityProviderId))
        }
      }
    }

    "allow to get identity provider config by id" in {
      testIt { tested =>
        val cfg = config()
        val id = randomId()
        for {
          res1 <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
          res3 <- tested.getIdentityProviderConfig(id)
        } yield {
          res1 shouldBe Right(cfg)
          res2 shouldBe Right(cfg)
          res3 shouldBe Left(IdentityProviderConfigNotFound(id))
        }
      }
    }

    "allow to get identity provider config by issuer" in {
      testIt { tested =>
        val cfg = config()
        val nonExistingIssuer = "issuer_which_does_not_exist"
        for {
          res1 <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.getIdentityProviderConfig(cfg.issuer)
          res3 <- tested.getIdentityProviderConfig(nonExistingIssuer)
        } yield {
          res1 shouldBe Right(cfg)
          res2 shouldBe Right(cfg)
          res3 shouldBe Left(IdentityProviderConfigByIssuerNotFound(nonExistingIssuer))
        }
      }
    }

    "allow to get active identity provider config by issuer" in {
      testIt { tested =>
        val cfg = config()
        val deactivatedConfig = config().copy(isDeactivated = true)
        val nonExistingIssuer = "issuer_which_does_not_exist"
        for {
          res1 <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.createIdentityProviderConfig(deactivatedConfig)
          res3 <- tested.getActiveIdentityProviderByIssuer(cfg.issuer)
          res4 <- tested.getActiveIdentityProviderByIssuer(nonExistingIssuer).failed
          res5 <- tested.getActiveIdentityProviderByIssuer(deactivatedConfig.issuer).failed
        } yield {
          res1 shouldBe Right(cfg)
          res2 shouldBe Right(deactivatedConfig)
          res3 shouldBe cfg
          res4 shouldBe an[Exception]
          res5 shouldBe an[Exception]
        }
      }
    }

    "allow to check if identity provider config by id exists" in {
      testIt { tested =>
        val cfg = config()
        val id = randomId()
        for {
          res1 <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.identityProviderConfigExists(cfg.identityProviderId)
          res3 <- tested.identityProviderConfigExists(id)
        } yield {
          res1 shouldBe Right(cfg)
          res2 shouldBe true
          res3 shouldBe false
        }
      }
    }

    "fail to delete non-existing identity provider config" in {
      val id = randomId()
      testIt { tested =>
        for {
          res <- tested.deleteIdentityProviderConfig(id)
        } yield {
          res shouldBe Left(IdentityProviderConfigNotFound(id))
        }
      }
    }

    "allows to update nothing" in {
      testIt { tested =>
        val cfg = config()
        for {
          _ <- tested.createIdentityProviderConfig(cfg)
          res <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId
            )
          )
        } yield {
          res shouldBe Right(cfg)
        }
      }
    }

    "fail to update non existing config" in {
      val id = randomId()
      testIt { tested =>
        for {
          res <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = id
            )
          )
        } yield {
          res shouldBe Left(IdentityProviderConfigNotFound(id))
        }
      }
    }

    "allows to update existing identity provider config's isDeactivated attribute" in {
      testIt { tested =>
        val cfg = config().copy(isDeactivated = false)
        for {
          _ <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId,
              isDeactivatedUpdate = Some(true),
            )
          )
          res3 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
        } yield {
          res2 shouldBe Right(cfg.copy(isDeactivated = true))
          res3 shouldBe Right(cfg.copy(isDeactivated = true))
        }
      }
    }

    "allows to update existing identity provider config's jwksUrl attribute" in {
      testIt { tested =>
        val cfg = config().copy(jwksUrl = JwksUrl.assertFromString("http://daml.com/jwks1.json"))
        for {
          _ <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId,
              jwksUrlUpdate = Some(JwksUrl.assertFromString("http://daml.com/jwks2.json")),
            )
          )
          res3 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
        } yield {
          val expected = cfg.copy(jwksUrl = JwksUrl.assertFromString("http://daml.com/jwks2.json"))
          res2 shouldBe Right(expected)
          res3 shouldBe Right(expected)
        }
      }
    }

    "allows to update existing identity provider config's audience attribute" in {
      testIt { tested =>
        val cfg = config().copy(audience = Some("audience1"))
        for {
          _ <- tested.createIdentityProviderConfig(config())
          _ <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId,
              audienceUpdate = Some(Some("audience2")),
            )
          )
          res3 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
          // no update
          res4 <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId,
              audienceUpdate = None,
            )
          )
          res5 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
          // unset the value
          res6 <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId,
              audienceUpdate = Some(None),
            )
          )
          res7 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
        } yield {
          res2 shouldBe Right(cfg.copy(audience = Some("audience2")))
          res3 shouldBe Right(cfg.copy(audience = Some("audience2")))

          res4 shouldBe Right(cfg.copy(audience = Some("audience2")))
          res5 shouldBe Right(cfg.copy(audience = Some("audience2")))

          res6 shouldBe Right(cfg.copy(audience = None))
          res7 shouldBe Right(cfg.copy(audience = None))
        }
      }
    }

    "allows to update existing identity provider config's issuer attribute" in {
      testIt { tested =>
        val cfg = config().copy(issuer = "issuer1")
        for {
          _ <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId,
              issuerUpdate = Some("issuer2"),
            )
          )
          res3 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
        } yield {
          res2 shouldBe Right(cfg.copy(issuer = "issuer2"))
          res3 shouldBe Right(cfg.copy(issuer = "issuer2"))
        }
      }
    }

    "allows to update existing identity provider config's issuer attribute to the same value" in {
      testIt { tested =>
        val cfg = config().copy(issuer = "issuer1")
        for {
          _ <- tested.createIdentityProviderConfig(cfg)
          res2 <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId,
              issuerUpdate = Some("issuer1"),
            )
          )
          res3 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
        } yield {
          res2 shouldBe Right(cfg.copy(issuer = "issuer1"))
          res3 shouldBe Right(cfg.copy(issuer = "issuer1"))
        }
      }
    }

    "allows to update everything at the same time" in {
      testIt { tested =>
        val id = randomId()
        val cfg = IdentityProviderConfig(
          identityProviderId = id,
          isDeactivated = false,
          jwksUrl = JwksUrl.assertFromString("http://example.com/jwks.json"),
          issuer = UUID.randomUUID().toString,
          audience = Some(UUID.randomUUID().toString),
        )
        for {
          _ <- tested.createIdentityProviderConfig(cfg)
          res <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg.identityProviderId,
              issuerUpdate = Some("issuer2"),
              jwksUrlUpdate = Some(JwksUrl.assertFromString("http://daml.com/jwks2.json")),
              isDeactivatedUpdate = Some(true),
              audienceUpdate = Some(Some("aud2")),
            )
          )
          res3 <- tested.getIdentityProviderConfig(cfg.identityProviderId)
        } yield {
          val expected = IdentityProviderConfig(
            identityProviderId = id,
            isDeactivated = true,
            jwksUrl = JwksUrl.assertFromString("http://daml.com/jwks2.json"),
            issuer = "issuer2",
            audience = Some("aud2"),
          )
          res shouldBe Right(expected)
          res3 shouldBe Right(expected)
        }
      }
    }

    "disallow updating issuer to non-unique value" in {
      testIt { tested =>
        val cfg1 = config().copy(issuer = "issuer1")
        val cfg2 = config().copy(issuer = "issuer2")
        for {
          _ <- tested.createIdentityProviderConfig(cfg1)
          _ <- tested.createIdentityProviderConfig(cfg2)
          res <- tested.updateIdentityProviderConfig(
            IdentityProviderConfigUpdate(
              identityProviderId = cfg1.identityProviderId,
              issuerUpdate = Some("issuer2"),
            )
          )
        } yield {
          res shouldBe Left(IdentityProviderConfigWithIssuerExists("issuer2"))
        }
      }
    }

    "allow listing all configs" in {
      testIt { tested =>
        val cfg1 = config().copy(issuer = "issuer1")
        val cfg2 = config().copy(issuer = "issuer2")
        for {
          _ <- tested.createIdentityProviderConfig(cfg1)
          _ <- tested.createIdentityProviderConfig(cfg2)
          res <- tested.listIdentityProviderConfigs()
        } yield {
          res.value should contain theSameElementsAs Vector(cfg1, cfg2)
        }
      }
    }
  }
}
