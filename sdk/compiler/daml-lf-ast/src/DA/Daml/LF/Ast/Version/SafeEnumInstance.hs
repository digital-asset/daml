-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Orphan 'R.SafeEnum' instance for 'Version'.
--
-- The instance lives in its own module so that the @-Wno-orphans@ pragma
-- needed for it is scoped tightly to the orphan itself, and a quick
-- @grep -r Wno-orphans@ lands directly on the offender.
{-# OPTIONS_GHC -Wno-orphans #-}

module DA.Daml.LF.Ast.Version.SafeEnumInstance () where

import           Data.List (find, sort)

import qualified DA.Daml.LF.Ast.Range as R
import           DA.Daml.LF.Ast.Version.VersionType
import           DA.Daml.LF.Ast.Version.GeneratedVersions (allLfVersions)

-- | Sorting defensively in case 'allLfVersions' is not already sorted.
sortedLfVersions :: [Version]
sortedLfVersions = sort allLfVersions

-- | Reversed cache, so 'safePred' doesn't re-reverse the list on every call.
reverseSortedLfVersions :: [Version]
reverseSortedLfVersions = reverse sortedLfVersions

instance R.SafeEnum Version where
  safeSucc v = find (> v) sortedLfVersions
  safePred v = find (< v) reverseSortedLfVersions


