-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker
  ( Error (..)
  , checkModule
  , checkPackage
  , errorLocation
  , ValueCheck(..)
  , defaultValueCheck
  ) where

import           DA.Prelude

import           Data.Either.Combinators           (mapLeft)
import           Data.Foldable

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.TypeChecker.Check      as Check
import qualified DA.Daml.LF.TypeChecker.Value as Value
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import qualified DA.Daml.LF.TypeChecker.PartyLiterals as PartyLits
import qualified DA.Daml.LF.TypeChecker.Recursion as Recursion
import qualified DA.Daml.LF.TypeChecker.Serializability as Serializability

data ValueCheck =
    PerformValueCheck
  | UnsafeSkipValueCheck
  deriving (Eq, Ord, Show)

-- | note that we eventually want to _always_ perform the value check. however right now
-- the value check is too much of a burden for most codebases, so in most places we make
-- it opt-in. we plan to make it mandatory once we can reliably evaluate every top-level
-- definition at compile time.
defaultValueCheck  :: ValueCheck
defaultValueCheck = UnsafeSkipValueCheck

checkModule ::
     World
  -> Version
  -> Module
  -> ValueCheck
  -> Either Error ()
checkModule world0 version m valueCheck = do
    runGamma (extendWorld m world0) version $ do
      Check.checkModule m
      Recursion.checkModule m
      Serializability.checkModule m
      PartyLits.checkModule m
      case valueCheck of
        PerformValueCheck -> Value.checkModule m
        UnsafeSkipValueCheck -> return ()

-- | Type checks a whole DAML-LF package. Assumes the modules in the package are
-- sorted topologically, i.e., module @A@ appears before module @B@ whenever @B@
-- depends on @A@.
checkPackage :: [(PackageId, Package)] -> Package -> ValueCheck -> Either Error ()
checkPackage pkgDeps pkg valueCheck = do
    Package version mods <- mapLeft EImportCycle (topoSortPackage pkg)
    let check1 world0 mod0 = do
          checkModule world0 version mod0 valueCheck
          pure (extendWorld mod0 world0)
    void (foldlM check1 (initWorld pkgDeps version) mods)
