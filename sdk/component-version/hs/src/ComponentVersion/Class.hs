-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}

module ComponentVersion.Class
  ( ComponentVersioned
  , componentVersionString
  , componentVersionMvn
  , componentVersionDamlPackage
  , damlStdlib
  , ComponentVersions (..)
  , withComponentVersions'
  , getComponentVersion
  ) where

import Control.Exception (throw)
import DA.Daml.Version.Types (ComponentVersion(..), parseComponentVersion)
import Module (UnitId, stringToUnitId)
import Unsafe.Coerce (unsafeCoerce)

import qualified Data.Text as T (pack)

data ComponentVersions = ComponentVersions
  { _componentVersionRaw :: String
  , _componentVersionMvn :: String
  , _componentVersionDamlPackage :: String
  }

class ComponentVersioned where
  componentVersions :: ComponentVersions

newtype WithComponentVersions r = WithComponentVersions (ComponentVersioned => r)

-- | Used to supply custom version values to a function with an `ComponentVersioned` constraint.
withComponentVersions' :: forall r. ComponentVersions -> (ComponentVersioned => r) -> r
withComponentVersions' v k =
  -- This mirrors `reflection:Data.Reflection.give`, replacing `Given a` with
  -- `ComponentVersioned` and `a` with `ComponentVersions`.
  -- Essentially, we're casting `k`
  --   from: `ComponentVersioned => r` (through newtype `WithComponentVersions r`)
  --   to:   `ComponentVersions  -> r`
  -- This works because
  --   1. At runtime, constraint arrows and function arrows have the same representation, and
  --   2. ComponentVersioned has a single method, so its dictionary has the same representation as the type of that method.
  -- See: https://hackage.haskell.org/package/reflection/docs/Data-Reflection.html#v:give
  unsafeCoerce
    @(WithComponentVersions r)
    @(ComponentVersions -> r)
    (WithComponentVersions k :: WithComponentVersions r)
    v
{-# NOINLINE withComponentVersions' #-}

componentVersionString :: ComponentVersioned => String
componentVersionString = _componentVersionRaw componentVersions

componentVersionMvn :: ComponentVersioned => String
componentVersionMvn = _componentVersionMvn componentVersions

componentVersionDamlPackage :: ComponentVersioned => String
componentVersionDamlPackage = _componentVersionDamlPackage componentVersions

damlStdlib :: ComponentVersioned => UnitId
damlStdlib = stringToUnitId ("daml-stdlib-" ++ componentVersionDamlPackage)

getComponentVersion :: ComponentVersioned => ComponentVersion
getComponentVersion = either throw id $ parseComponentVersion $ T.pack componentVersionString
