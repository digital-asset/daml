-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Error
    ( Error(..)
    ) where

import qualified Data.Text as T
import Data.Int (Int32)

import DA.Daml.LF.Ast

data Error
  = MissingField String
  | UnknownEnum String Int32
  | ParseError String
  | DuplicateModule ModuleName
  | DuplicateTypeSyn TypeSynName
  | DuplicateDataType TypeConName
  | DuplicateValue ExprValName
  | EDuplicateTemplate TypeConName
  | DuplicateChoice ChoiceName
  | DuplicateMethod MethodName
  | DuplicateException TypeConName
  | DuplicateInterface TypeConName
  | DuplicateRequires (Qualified TypeConName)
  | DuplicateImplements (Qualified TypeConName)
  | DuplicateCoImplements (Qualified TypeConName)
  | UnsupportedMinorVersion T.Text
  | BadStringId Int32
  | BadDottedNameId Int32
  | BadTypeId Int32
  | ExpectedTCon Type
  deriving (Show, Eq)
