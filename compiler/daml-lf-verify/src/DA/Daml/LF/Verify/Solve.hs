-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Constraint solver for DAML LF static verification
module DA.Daml.LF.Verify.Solve
  ( constructConstr
  , ConstraintSet(..)
  ) where

import Data.Maybe (fromJust)
import Data.List (lookup)
import qualified Data.Text as T
import SimpleSMT
-- import Z3.Monad

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify.Context
-- import DA.Pretty

-- | A simple form of expressions featuring basic arithmetic.
data ConstraintExpr
  -- | Reference to an expression variable.
  = CVar !ExprVarName
  -- | Sum of two expressions.
  | CAdd !ConstraintExpr !ConstraintExpr
  -- | Subtraction of two expressions.
  | CSub !ConstraintExpr !ConstraintExpr

instance Show ConstraintExpr where
  show (CVar x) = T.unpack $ unExprVarName x
  show (CAdd e1 e2) = show e1 ++ " + " ++ show e2
  show (CSub e1 e2) = show e1 ++ " - " ++ show e2

exp2CExp :: Expr -> ConstraintExpr
exp2CExp (EVar x) = CVar x
exp2CExp (ERecProj _ f (EVar x)) = CVar $ recProj2Var x f
exp2CExp (ETmApp (ETmApp (ETyApp (EBuiltin b) _) e1) e2) = case b of
  BEAddNumeric -> CAdd (exp2CExp e1) (exp2CExp e2)
  BESubNumeric -> CSub (exp2CExp e1) (exp2CExp e2)
  _ -> error ("Builtin: " ++ show b)
exp2CExp e = error ("Conversion: " ++ show e)
-- exp2CExp e = error ("Conversion: " ++ renderPretty e)

skol2var :: Skolem -> [ExprVarName]
skol2var (SkolVar x) = [x]
skol2var (SkolRec x fs) = map (recProj2Var x) fs

recProj2Var :: ExprVarName -> FieldName -> ExprVarName
recProj2Var (ExprVarName x) (FieldName f) = ExprVarName (x `T.append` "." `T.append` f)

-- | The set of constraints to be solved.
data ConstraintSet = ConstraintSet
  { _cVars :: ![ExprVarName]
    -- ^ The variables to be declared.
  , _cCres :: ![ConstraintExpr]
    -- ^ The field values of all newly created instances.
  , _cArcs :: ![ConstraintExpr]
    -- ^ The field values of all archived instances.
  }
  deriving Show

-- | Constructs a constraint set from the generator environment, together with
-- the template name, the choice and field to be verified.
-- TODO: Take choices into account?
constructConstr :: Env -> TypeConName -> ChoiceName -> FieldName -> ConstraintSet
constructConstr env tem ch f =
  case lookupChoInHMap (_envchs env) ch of
    Just upds ->
      let vars = concat $ map skol2var $ _envskol env
          creUpds = filter (\UpdCreate{..} -> tem == qualObject _creTemp) (_usCre upds)
          creVals = map (exp2CExp . fromJust . (lookup f) . _creField) creUpds
          arcUpds = filter (\UpdArchive{..} -> tem == qualObject _arcTemp) (_usArc upds)
          arcVals = map (exp2CExp . fromJust . (lookup f) . _arcField) arcUpds
      in ConstraintSet vars creVals arcVals
    Nothing -> error "Choice not found"

-- cexp2Z3 :: ConstraintExpr -> Z3 AST
-- cexp2Z3 (CVar x) = undefined
-- cexp2Z3 (CAdd e1 e2) = do
--   ze1 <- cexp2Z3 e1
--   ze2 <- cexp2Z3 e2
--   mkAdd [ze1, ze2]
-- cexp2Z3 (CSub e1 e2) = do
--   ze1 <- cexp2Z3 e1
--   ze2 <- cexp2Z3 e2
--   mkSub [ze1, ze2]

-- -- TODO: This really shouldn't be an Integer
-- ctr2Z3 :: ConstraintSet -> Z3 (Maybe [Integer])
-- ctr2Z3 ConstraintSet{..} = do
--   -- TODO: Declare variables
--   cres <- mapM cexp2Z3 _cCres >>= mkAdd
