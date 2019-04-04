-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.LanguageServer.TH where

import Data.Char
import "template-haskell" Language.Haskell.TH (Name, Q, Dec)
import qualified Data.Aeson.TH as Aeson.TH

deriveJSON :: Name -> Q [Dec]
deriveJSON name = Aeson.TH.deriveJSON options name
  where
    options = Aeson.TH.defaultOptions
     { Aeson.TH.fieldLabelModifier = modifyLabel
     , Aeson.TH.omitNothingFields = True
     }

    modifyLabel label =
       case dropWhile isLower label of
         (x:xs) -> toLower x : xs
         _ -> []

