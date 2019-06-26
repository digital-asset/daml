-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Error
    ( Decode
    , Error(..)
    ) where

import qualified Data.Text as T
import Data.Word (Word64)

import DA.Daml.LF.Ast

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
  | MissingPackageRefId Word64
  deriving (Show, Eq)

type Decode = Either Error
