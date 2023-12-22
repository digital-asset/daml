-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}

module SdkVersion.Class
  ( SdkVersioned
  , sdkVersion
  , mvnVersion
  , sdkPackageVersion
  , damlStdlib
  , SdkVersions (..)
  , withSdkVersions'
  ) where

import Module (UnitId, stringToUnitId)
import Unsafe.Coerce (unsafeCoerce)

data SdkVersions = SdkVersions
  { _sdkVersion :: String
  , _mvnVersion :: String
  , _sdkPackageVersion :: String
  }

class SdkVersioned where
  sdkVersions :: SdkVersions

newtype WithSdkVersions r = WithSdkVersions (SdkVersioned => r)

-- | Used to supply custom version values to a function with an `SdkVersioned` constraint.
withSdkVersions' :: forall r. SdkVersions -> (SdkVersioned => r) -> r
withSdkVersions' v k =
  -- This mirrors `reflection:Data.Reflection.give`, replacing `Given a` with
  -- `SdkVersioned` and `a` with `SdkVersions`.
  -- Essentially, we're casting `k`
  --   from: `SdkVersioned => r` (through newtype `WithSdkVersions r`)
  --   to:   `SdkVersions  -> r`
  -- This works because
  --   1. At runtime, constraint arrows and function arrows have the same representation, and
  --   2. SdkVersioned has a single method, so its dictionary has the same representation as the type of that method.
  -- See: https://hackage.haskell.org/package/reflection/docs/Data-Reflection.html#v:give
  unsafeCoerce
    @(WithSdkVersions r)
    @(SdkVersions -> r)
    (WithSdkVersions k :: WithSdkVersions r)
    v
{-# NOINLINE withSdkVersions' #-}

sdkVersion :: SdkVersioned => String
sdkVersion = _sdkVersion sdkVersions

mvnVersion :: SdkVersioned => String
mvnVersion = _mvnVersion sdkVersions

sdkPackageVersion :: SdkVersioned => String
sdkPackageVersion = _sdkPackageVersion sdkVersions

damlStdlib :: SdkVersioned => UnitId
damlStdlib = stringToUnitId ("daml-stdlib-" ++ sdkPackageVersion)
