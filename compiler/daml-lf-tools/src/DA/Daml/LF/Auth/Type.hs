-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Types for the constraints, location information and errors.
module DA.Daml.LF.Auth.Type where

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Pretty
import DA.Pretty


data Frame = FrameFunction (Qualified ExprValName)
           | FrameTemplate (Qualified TypeConName)
           | FrameChoice ChoiceName
             deriving Show

data Ctx = CtxLet ExprVarName Expr
         | CtxCase CasePattern Expr
         -- | CtxFetch ExprVarName Expr -- probably required to accumulate fetch info
           deriving Show

pattern CtxTrue :: Expr -> Ctx
pattern CtxTrue x = CtxCase (CPEnumCon ECTrue) x

data Pred = PredNotNull  Expr
          | PredSubsetEq Expr [Expr]
            deriving Show


data Error
  = EFailedToSolve [Frame] [Ctx] Pred
  | EUnknownTemplate (Qualified TypeConName)
  | EUnknownChoice (Qualified TypeConName) ChoiceName
  | EUnknownValue (Qualified ExprValName)
  deriving Show


instance Pretty Error where
  pPrint = \case
    EFailedToSolve loc ctx p -> "Failed to solve constraint" $$ vcat (map (\x -> "* " <> pretty x) loc) $$ pretty (toExpr ctx p)
    EUnknownTemplate t -> "Unknown template: " <> prettyQualified prettyDottedName t
    EUnknownChoice t c -> "Unknown choice: " <> prettyQualified prettyDottedName t <> " " <> prettyName c
    EUnknownValue x -> "Unknown value: " <> prettyQualified prettyName x

instance Pretty Frame where
  pPrint (FrameFunction x) = prettyQualified prettyName x
  pPrint (FrameTemplate x) = "template " <> prettyQualified prettyDottedName x
  pPrint (FrameChoice x) = "choice " <> prettyName x


toExpr :: [Ctx] -> Pred -> Expr
toExpr [] (PredNotNull x) = ETmApp (EBuiltin $ BEText "NOT_NULL?") x
toExpr [] (PredSubsetEq x xs) = mkETmApps (EBuiltin $ BEText "SUBSET_EQ?") $ x:xs
toExpr (CtxLet a b:xs) p = ELet (Binding (a, TUnit) b) $ toExpr xs p
toExpr (CtxCase a b:xs) p = ECase b [CaseAlternative a $ toExpr xs p]
