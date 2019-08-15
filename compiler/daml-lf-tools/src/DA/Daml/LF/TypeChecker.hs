-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker
  ( Error (..)
  , checkModule
  , errorLocation
  ) where

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
    runGamma (extendWorldSelf m world0) version $ do
      Check.checkModule m
      Recursion.checkModule m
      Serializability.checkModule m
      PartyLits.checkModule m
