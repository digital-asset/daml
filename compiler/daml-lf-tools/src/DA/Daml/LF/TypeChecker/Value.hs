-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ApplicativeDo #-}
-- | Functions to check if a DAML-LF expression is a DAML-LF value. See
-- DAML-LF specification for a definition of value.
module DA.Daml.LF.TypeChecker.Value (checkModule) where

import qualified Data.Text as T
import GHC.Stack (HasCallStack)
import Data.Traversable (for)
import Control.Lens (over, _1)
import Data.Either.Validation
import Data.Foldable (traverse_)
import Data.Fixed (Fixed)

import DA.Prelude
import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error

-- TODO (FM) consider allowing TyApp since it doesn't matter for evaluation
data Value =
    VRecord !TypeConApp ![(FieldName, Value)]
  | VVariant !TypeConApp !VariantConName !Value
  | VTuple ![(FieldName, Value)]
  | VTmLam !(ExprVarName, Type) !Expr
  -- ^ expressions are allowed inside functions -- otherwise we wouldn't be able
  -- to define top level functions that compute.
  | VTyLam !(TypeVarName, Kind) !Value
  -- ^ for type abstractions we force the body to be a value, so that the values
  -- will still be values if we perform type erasure.
  | VUpdate !Update
  -- ^ updates are akin to functions -- we allow them at the top level
  -- to allow composition, under the assumption that interpreters can compile them.
  | VScenario !Scenario
  -- ^ see comment for 'VUpdate'
  | VCons !Type !Value !Value
  | VNil !Type
  | VEnum !EnumCon
  | VInt64    !Int64
  | VDecimal    !(Fixed E10)
  | VText       !T.Text
  | VTimestamp  !Int64
  | VParty      !PartyLiteral
  | VVal !(Qualified ExprValName)
  | VOptional !Type !(Maybe Value)
  deriving (Eq, Show)

-- | assumes the given Expr is closed. crashes otherwise.
--
-- if the given expression is _not_ a value, lists the subexpressions which
-- are not values and why they aren't.
--
-- note that we implement some simplification rules for this check to not
-- get in peoples' way too much. most notably, we perform constant folding
-- in 'DA.Daml.LF.Simplifier', so that stuff like @def x: Integer = 1 + 2@
-- works.
exprToValue :: HasCallStack => Expr -> Validation [(String, Expr)] Value
-- note: converting the Expr to a Value is not strictly needed here, since
-- we do not do anything with it. however we define and use Value for two
-- reasons:
--
-- * have a clear haskell representation of what a DAML-LF value is;
-- * dramatically reducing the possibility of screwing up this function by
--   forcing the programmer to reify evidence that we do indeed have a value
--   on our hands.
exprToValue e00 =
  over (_Failure.traverse._1) ("Unexpected " ++) (go e00)

  where
    go = \case
      EVar var -> error ("variable" <> T.unpack (untag var))
      ERecCon tyCon fieldsE -> do
        fieldsV <- for fieldsE $ \(fldName, expr) -> do
          val <- go expr
          return (fldName, val)
        return (VRecord tyCon fieldsV)
      EVariantCon tyCon dataCon expr -> VVariant tyCon dataCon <$> go expr
      ETupleCon fieldsE -> fmap VTuple $ for fieldsE $ \(fldName, expr) -> do
        val <- go expr
        pure (fldName, val)
      ETyLam binder body -> VTyLam binder <$> go body
      ETmLam binder body -> pure (VTmLam binder body)
      ENil ty -> pure (VNil ty)
      ECons ty headE tailE -> VCons ty <$> go headE <*> go tailE
      EUpdate upd -> pure (VUpdate upd)
      EScenario scen -> pure (VScenario scen)
      e@(EBuiltin bltin) ->
        case bltin of
          BEInt64 x -> pure (VInt64 x)
          BEDecimal x -> pure (VDecimal x)
          BEText x -> pure (VText x)
          BETimestamp x -> pure (VTimestamp x)
          BEParty x -> pure (VParty x)
          BEEnumCon x -> pure (VEnum x)
         -- NOTE(FM): we currently do not allow partially applied builtins
          -- to be values. this is debatable and we might revise it in the future,
          -- but we start with the more conservative option.
          _ -> Failure [("builtin function", e)]
      EVal ref ->  pure (VVal ref)
      ESome ty body -> VOptional ty . Just <$> go body
      ENone ty -> pure (VOptional ty Nothing)
      e@ERecProj{} -> Failure [("record projection", e)]
      e@ETupleProj{} -> Failure [("anonymous record projection", e)]
      e@ETyApp{} -> Failure [("type application", e)]
      e@ELet{} -> Failure [("let binding", e)]
      e@ECase{} -> Failure [("case statement", e)]
      e@ETmApp{} -> Failure [("application", e)]
      e@ERecUpd{} -> Failure [("record update", e)]
      e@ETupleUpd{} -> Failure [("tuple update", e)]
      ELocation _loc e -> go e

checkDefValue :: MonadGamma m => Module -> DefValue -> m ()
checkDefValue mod0 def =
  withContext
    (ContextDefValue mod0 def)
    (do
      case exprToValue (dvalBody def) of
        Failure badSubExprs -> throwWithContext (ENonValueDefinition badSubExprs)
        Success _expr -> return ())

checkModule :: MonadGamma m => Module -> m ()
checkModule mod0 = traverse_ (checkDefValue mod0) (moduleValues mod0)
