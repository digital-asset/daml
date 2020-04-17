-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Evaluator.Pretty
  ( ppExp
  ) where

import DA.Daml.LF.Evaluator.Exp (Var,Exp(..),Alt(..),TyVar,Type)
import DA.Daml.LF.Evaluator.Value (Value(..),B0(..),FieldName,Tag(..))
import Data.List (intercalate)
import qualified DA.Daml.LF.Ast as LF
import qualified Data.Text as T

import DA.Pretty (renderPretty)

ppValue :: Value -> String
ppValue = \case
  Function _ -> "<func>"
  TFunction _ -> "<tfunc>"
  Record elems -> "{"<> intercalate ","
    (map (\(name,v) -> ppFieldName name <> " = " <> ppValue v) elems) <> "}"
  Constructed tag args -> unTag tag <> ppArgs (map ppValue args)
  B0 b -> ppB0 b
  B1 b -> show b
  B2 b -> show b
  B2_1 b v -> "(" <> show b <> show v <> ")"
  B3 b -> show b
  B3_1 b v1 -> "(" <> show b <> show v1 <> ")"
  B3_2 b v1 v2 -> "(" <> show b <> show v1 <> " " <> show v2 <> ")"
  where

    ppB0 :: B0 -> String
    ppB0 = \case
        Unit -> "()"
        Num i -> show i
        Text t -> show t

ppVar :: Var -> String
ppVar = T.unpack . LF.unExprVarName

ppFieldName :: FieldName -> String
ppFieldName = T.unpack . LF.unFieldName

ppArgs :: [String] -> String
ppArgs = \case
  [] -> ""
  args -> "(" <> intercalate "," args <> ")"

ppExp :: Exp -> String
ppExp = pp0
  where

    pp0 :: Exp -> String -- called from context which does NOT need brackets, i.e. the top
    pp0 = \case
      Lit v -> ppValue v
      Var x -> ppVar x
      App e1 e2 -> pp0 e1 <> " " <> pp1 e2
      Lam x typ body -> "\\(" <> ppVar x <> ":" <> ppType typ <> ") -> " <> pp0 body
      Let x rhs body -> "let " <> ppVar x <> " = " <> pp0 rhs <> " in " <> pp0 body
      Rec elems -> "{"<> intercalate ","
        (map (\(name,exp) -> ppFieldName name <> " = " <> pp0 exp) elems) <> "}"
      Dot exp name -> pp1 exp <> "." <> ppFieldName name
      Con tag elems -> unTag tag <> ppArgs (map pp0 elems)
      Match{scrut,alts} -> "(case " <> pp0 scrut <> " of " <> intercalate "; " (map ppAlt alts) <> ")"
      Ref i -> "#" <> show i
      TypeLam tv exp -> "\\[" <> ppTV tv <> "]." <> pp1 exp
      TypeApp exp typ -> pp0 exp <> "[" <> ppType typ <> "]"

    ppAlt :: Alt -> String
    ppAlt Alt{tag,bound,rhs} = unTag tag <> ppArgs (map ppVar bound) <> " -> " <> pp0 rhs

    pp1 :: Exp -> String
    pp1 exp = (if atomic exp then pp0 else brace . pp0) exp

    brace :: String -> String
    brace s = "(" <> s <> ")"

    atomic :: Exp -> Bool
    atomic = \case
      Lit{} -> True
      Var{} -> True
      Rec{} -> True
      Ref{} -> True

      App{} -> False
      Lam{} -> False
      Let{} -> False
      Dot{} -> False
      Con{} -> False
      Match{} -> False
      TypeLam{} -> False
      TypeApp{} -> False


ppTV :: TyVar -> String
ppTV = T.unpack . LF.unTypeVarName

ppType :: Type -> String
ppType = renderPretty

