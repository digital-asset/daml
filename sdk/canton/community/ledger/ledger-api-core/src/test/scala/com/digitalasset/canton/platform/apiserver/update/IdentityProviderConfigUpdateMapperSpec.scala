// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.digitalasset.canton.ledger.api.{IdentityProviderId, JwksUrl}
import com.digitalasset.canton.ledger.localstore.api.IdentityProviderConfigUpdate
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.EitherValues
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class IdentityProviderConfigUpdateMapperSpec extends AnyFreeSpec with Matchers with EitherValues {

  private val id1: IdentityProviderId.Id =
    IdentityProviderId.Id(LedgerString.assertFromString("idp1"))

  def makeConfigUpdate(
      identityProviderId: IdentityProviderId.Id = id1,
      isDeactivatedUpdate: Option[Boolean] = None,
      jwksUrlUpdate: Option[JwksUrl] = None,
      issuerUpdate: Option[String] = None,
      audienceUpdate: Option[Option[String]] = None,
  ): IdentityProviderConfigUpdate = IdentityProviderConfigUpdate(
    identityProviderId = identityProviderId,
    isDeactivatedUpdate = isDeactivatedUpdate,
    jwksUrlUpdate = jwksUrlUpdate,
    issuerUpdate = issuerUpdate,
    audienceUpdate = audienceUpdate,
  )

  val emptyConfigUpdate: IdentityProviderConfigUpdate = makeConfigUpdate()

  val config = makeConfigUpdate(
    isDeactivatedUpdate = Some(false),
    jwksUrlUpdate = Some(JwksUrl("http://url.com/")),
    issuerUpdate = Some("issuer"),
    audienceUpdate = Some(Some("audience")),
  )

  "map to idp updates" - {
    "basic mapping" - {
      "with all individual fields to update listed in the update mask" in {
        IdentityProviderConfigUpdateMapper
          .toUpdate(
            config,
            FieldMask(
              Seq("is_deactivated", "issuer", "jwks_url", "audience")
            ),
          )
          .value shouldBe makeConfigUpdate(
          isDeactivatedUpdate = Some(false),
          jwksUrlUpdate = Some(JwksUrl("http://url.com/")),
          issuerUpdate = Some("issuer"),
          audienceUpdate = Some(Some("audience")),
        )
      }

      "with audience" in {
        IdentityProviderConfigUpdateMapper
          .toUpdate(config, FieldMask(Seq("audience")))
          .value shouldBe makeConfigUpdate(audienceUpdate = Some(Some("audience")))

        IdentityProviderConfigUpdateMapper
          .toUpdate(config.copy(audienceUpdate = None), FieldMask(Seq("audience")))
          .value shouldBe makeConfigUpdate(audienceUpdate = Some(None))

        IdentityProviderConfigUpdateMapper
          .toUpdate(config.copy(audienceUpdate = Some(Some(""))), FieldMask(Seq("audience")))
          .value shouldBe makeConfigUpdate(audienceUpdate = Some(Some("")))

        IdentityProviderConfigUpdateMapper
          .toUpdate(config.copy(audienceUpdate = Some(None)), FieldMask(Seq("audience")))
          .value shouldBe makeConfigUpdate(audienceUpdate = Some(None))
      }

      "with is_deactivated" in {
        IdentityProviderConfigUpdateMapper
          .toUpdate(
            config,
            FieldMask(
              Seq("is_deactivated")
            ),
          )
          .value shouldBe makeConfigUpdate(
          isDeactivatedUpdate = Some(false)
        )
      }

      "with issuer" in {
        IdentityProviderConfigUpdateMapper
          .toUpdate(
            config,
            FieldMask(
              Seq("issuer")
            ),
          )
          .value shouldBe makeConfigUpdate(
          issuerUpdate = Some("issuer")
        )
      }

      "with jwks_url" in {
        IdentityProviderConfigUpdateMapper
          .toUpdate(
            config,
            FieldMask(
              Seq("jwks_url")
            ),
          )
          .value shouldBe makeConfigUpdate(
          jwksUrlUpdate = Some(JwksUrl("http://url.com/"))
        )
      }
    }
  }

  "produce an error when " - {
    "field masks lists unknown field" in {
      IdentityProviderConfigUpdateMapper
        .toUpdate(config, FieldMask(Seq("some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")

      IdentityProviderConfigUpdateMapper
        .toUpdate(config, FieldMask(Seq("some_unknown_field", "jwks_url")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")

      IdentityProviderConfigUpdateMapper
        .toUpdate(config, FieldMask(Seq("some_unknown_field", "issuer")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")
    }
    "specifying identity_provider_id in the update mask" in {
      IdentityProviderConfigUpdateMapper
        .toUpdate(
          config,
          FieldMask(
            Seq("identity_provider_id")
          ),
        )
        .value shouldBe emptyConfigUpdate
    }
    "empty field mask" in {
      IdentityProviderConfigUpdateMapper
        .toUpdate(
          config,
          FieldMask(Seq.empty),
        )
        .left
        .value shouldBe UpdatePathError.EmptyUpdateMask
    }
    "update path with invalid field path syntax" in {
      IdentityProviderConfigUpdateMapper
        .toUpdate(config, FieldMask(Seq(".issuer")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath(".issuer")
      IdentityProviderConfigUpdateMapper
        .toUpdate(config, FieldMask(Seq(".identity_provider_config.issuer")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath(".identity_provider_config.issuer")
    }
    "multiple update paths with the same field path" in {
      IdentityProviderConfigUpdateMapper
        .toUpdate(config, FieldMask(Seq("issuer", "issuer")))
        .left
        .value shouldBe UpdatePathError.DuplicatedFieldPath("issuer")
      IdentityProviderConfigUpdateMapper
        .toUpdate(config, FieldMask(Seq("jwks_url", "jwks_url", "issuer")))
        .left
        .value shouldBe UpdatePathError.DuplicatedFieldPath("jwks_url")
    }
  }
}
