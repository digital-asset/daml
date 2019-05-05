-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | An extended version of "Data.Aeson.TH", which provides our own
-- 'daAesonEncodingOptions' and the corresponding 'deriveDAJSON'.
module Data.Aeson.TH.Extended
    ( module Data.Aeson.TH
    , deriveDAToJSON
    , deriveDAFromJSON
    ) where


import           Data.Aeson.TH
import           Data.List  (stripPrefix)
import           Data.Char  (toLower)

import qualified "template-haskell" Language.Haskell.TH        as TH

import           Prelude


------------------------------------------------------------------------------
-- Additional error management utilitites
------------------------------------------------------------------------------

-- | Serialization options for da-code. Optimized towards short outputs
-- and fast encoding.
daAesonEncodingOptions :: String -> Options
daAesonEncodingOptions prefix = defaultOptions
    { sumEncoding       = ObjectWithSingleField
    , omitNothingFields = True
    , fieldLabelModifier = \label ->
        maybe label lowerHead $ stripPrefix prefix label
    }
  where
    lowerHead = \case
        (x:xs) -> toLower x : xs
        []     -> []

deriveDAToJSON :: String -> TH.Name -> TH.Q [TH.Dec]
deriveDAToJSON = deriveToJSON . daAesonEncodingOptions

deriveDAFromJSON :: String -> TH.Name -> TH.Q [TH.Dec]
deriveDAFromJSON = deriveFromJSON . daAesonEncodingOptions
