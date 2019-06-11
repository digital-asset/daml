-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Ledger.Valuable(
    Valuable(..), -- types which can be converted to/from a Ledger API Value
    ) where

import Data.Text.Lazy (Text)
import DA.Ledger.Types

class Valuable a where
    toValue :: a -> Value
    fromValue :: Value -> Maybe a

    toRecord :: a -> Record
    toRecord =
        Record Nothing
        . map (RecordField "") --(\v -> RecordField {label = "", value = v})
        . (\case VList vs -> vs; v -> [v])
        . toValue

    fromRecord :: Record -> Maybe a
    fromRecord =
        fromValue
        . VList
        . map fieldValue --(\RecordField{value} -> value)
        . fields

instance Valuable Int where
    toValue = VInt
    fromValue = \case VInt x -> Just x; _ -> Nothing

instance Valuable Party where
    toValue = VParty
    fromValue = \case VParty x -> Just x; _ -> Nothing

instance Valuable a => Valuable [a] where
    toValue = VList . map toValue
    fromValue = \case VList vs -> mapM fromValue vs; _ -> Nothing

instance Valuable Text where
    toValue = VString
    fromValue = \case VString x -> Just x; _ -> Nothing
