-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker
  ( Error (..)
  , checkModule
  , checkPackage
  , errorLocation
  ) where

import           DA.Prelude

import           Data.Either.Combinators           (mapLeft)
import           Data.Foldable

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.TypeChecker.Check      as Check
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error
import qualified DA.Daml.LF.TypeChecker.PartyLiterals as PartyLits
import qualified DA.Daml.LF.TypeChecker.Recursion as Recursion
import qualified DA.Daml.LF.TypeChecker.Serializability as Serializability

checkModule ::
     World
  -> Version
  -> Module
  -> Either Error ()
checkModule world0 version m = do
    runGamma (extendWorld m world0) version $ do
      Check.checkModule m
      Recursion.checkModule m
      Serializability.checkModule m
      PartyLits.checkModule m

-- | Type checks a whole DAML-LF package. Assumes the modules in the package are
-- sorted topologically, i.e., module @A@ appears before module @B@ whenever @B@
-- depends on @A@.
checkPackage :: [(PackageId, Package)] -> Package -> Either Error ()
checkPackage pkgDeps pkg = do
    Package version mods <- mapLeft EImportCycle (topoSortPackage pkg)
    let check1 world0 mod0 = do
          checkModule world0 version mod0
          pure (extendWorld mod0 world0)
    void (foldlM check1 (initWorld pkgDeps version) mods)
