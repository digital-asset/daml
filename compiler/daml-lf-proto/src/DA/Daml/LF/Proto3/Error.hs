-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}

module DA.Daml.LF.Proto3.Error
    ( Decode
    , Error(..)
    ) where

import qualified Data.Text as T

import DA.Daml.LF.Ast
import DA.Prelude

data Error
  = MissingField String
  | UnknownEnum String Int
  | ParseError String
  | DuplicateModule ModuleName
  | DuplicateDataType TypeConName
  | DuplicateValue ExprValName
  | EDuplicateTemplate TypeConName
  | DuplicateChoice ChoiceName
  | UnsupportedMinorVersion T.Text
  deriving (Show, Eq)

type Decode a = Either Error a
