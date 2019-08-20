-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Error
    ( Decode
    , Error(..)
    ) where

import qualified Data.Text as T
import Data.Int (Int32)
import Data.Word (Word64)

import DA.Daml.LF.Ast

data Error
  = MissingField String
  | UnknownEnum String Int32
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
