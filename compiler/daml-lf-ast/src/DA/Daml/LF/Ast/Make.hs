-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Smart constructors for the DAML-LF AST.
module DA.Daml.LF.Ast.Make where

import DA.Daml.LF.Ast.Base

mkTUnit :: Type
mkTUnit = TBuiltin (BTEnum ETUnit)

mkEUnit :: Expr
mkEUnit = EBuiltin (BEEnumCon ECUnit)

mkTTuple :: [(FieldName, Type)] -> Type
mkTTuple fields
  | null fields = mkTUnit
  | otherwise   = TTuple fields

mkETupleCon :: [(FieldName, Expr)] -> Expr
mkETupleCon fields
  | null fields = mkEUnit
  | otherwise   = ETupleCon fields
