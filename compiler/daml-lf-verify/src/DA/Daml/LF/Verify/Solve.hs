-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Constraint solver for DAML LF static verification
module DA.Daml.LF.Verify.Solve
  ( constructConstr
  , solveConstr
  , ConstraintSet(..)
  ) where

import Data.Maybe (fromJust)
import Data.List (lookup)
import Data.Set (toList, fromList)
import qualified Data.Text as T
import qualified SimpleSMT as S

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify.Context

-- TODO: Since S.SExpr is so similar, we could just drop this.
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

-- | Convert a DAML-LF expression to a constraint expression, if possible.
exp2CExp :: Expr
  -- ^ The expression to convert.
  -> ConstraintExpr
exp2CExp (EVar x) = CVar x
exp2CExp (ERecProj _ f (EVar x)) = CVar $ recProj2Var x f
exp2CExp (ETmApp (ETmApp (ETyApp (EBuiltin b) _) e1) e2) = case b of
  BEAddNumeric -> CAdd (exp2CExp e1) (exp2CExp e2)
  BESubNumeric -> CSub (exp2CExp e1) (exp2CExp e2)
  _ -> error ("Builtin: " ++ show b)
exp2CExp (ELocation _ e) = exp2CExp e
exp2CExp e = error ("Conversion: " ++ show e)

-- | Gather the variable names bound within a skolem variable.
skol2var :: Skolem
  -- ^ The skolem variable to handle.
  -> [ExprVarName]
skol2var (SkolVar x) = [x]
skol2var (SkolRec x fs) = map (recProj2Var x) fs

-- | Squash a record projection into a single variable name.
recProj2Var :: ExprVarName
  -- ^ The variable on which is being projected.
  -> FieldName
  -- ^ The field name which is being projected.
  -> ExprVarName
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
-- TODO: The choice and field don't actually need to be in the same template.
-- Take two templates as arguments.
constructConstr :: Env
  -- ^ The generator environment to convert.
  -> TypeConName
  -- ^ The template name to be verified.
  -> ChoiceName
  -- ^ The choice name to be verified.
  -> FieldName
  -- ^ The field name to be verified.
  -> ConstraintSet
constructConstr env tem ch f =
  case lookupChoInHMap (_envchs env) tem ch of
    Just (self, this, arg, updSubst) ->
      let upds = updSubst (EVar self) (EVar this) (EVar arg)
          vars = concatMap skol2var (_envskol env)
          creUpds = filter (\UpdCreate{..} -> tem == qualObject _creTemp) (_usCre upds)
          creVals = map (exp2CExp . fromJust . lookup f . _creField) creUpds
          arcUpds = filter (\UpdArchive{..} -> tem == qualObject _arcTemp) (_usArc upds)
          arcVals = map (exp2CExp . fromJust . lookup f . _arcField) arcUpds
      in ConstraintSet vars creVals arcVals
    Nothing -> error "Choice not found"

-- | Convert a constraint expression into an SMT expression from the solving library.
cexp2sexp :: [(ExprVarName,S.SExpr)]
  -- ^ The set of variable names, mapped to their corresponding SMT counterparts.
  -> ConstraintExpr
  -- ^ The constraint expression to convert.
  -> IO S.SExpr
cexp2sexp vars (CVar x) = case lookup x vars of
  Just exp -> return exp
  Nothing -> error ("Impossible: variable not found " ++ show x)
cexp2sexp vars (CAdd ce1 ce2) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  return $ S.add se1 se2
cexp2sexp vars (CSub ce1 ce2) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  return $ S.sub se1 se2

-- | Declare a list of variables for the SMT solver. Returns a list of the
-- declared variables, together with their corresponding SMT counterparts.
declareVars :: S.Solver
  -- ^ The SMT solver.
  -> [ExprVarName]
  -- ^ The variables to be declared.
  -> IO [(ExprVarName,S.SExpr)]
declareVars s xs = zip xs <$> mapM (\x -> S.declare s (var2str x) S.tReal) xs
  where
    var2str :: ExprVarName -> String
    var2str (ExprVarName x) = T.unpack x

-- | Solve a give constraint set. Prints 'unsat' when the constraint set is
-- valid. It asserts that the set of created and archived contracts are not
-- equal.
solveConstr :: FilePath
  -- ^ The path to the constraint solver.
  -> ConstraintSet
  -- ^ The constraint set to solve.
  -> IO ()
solveConstr spath ConstraintSet{..} = do
  log <- S.newLogger 1
  sol <- S.newSolver spath ["-in"] (Just log)
  vars <- declareVars sol $ filterDups _cVars
  cre <- foldl S.add (S.real 0) <$> mapM (cexp2sexp vars) _cCres
  arc <- foldl S.add (S.real 0) <$> mapM (cexp2sexp vars) _cArcs
  S.assert sol (S.not (cre `S.eq` arc))
  S.check sol >>= print
  where
    -- TODO: Filter vars beforehand
    -- TODO: Where does this "_" come from?
    filterDups :: [ExprVarName] -> [ExprVarName]
    filterDups = filter (\(ExprVarName x) -> x /= "_") . toList . fromList
