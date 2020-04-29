-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}

-- | Constraint solver for DAML LF static verification
module DA.Daml.LF.Verify.Solve
  ( constructConstr
  , solveConstr
  , ConstraintSet(..)
  ) where

import Data.Bifunctor
import Data.Maybe (fromJust, maybeToList)
import Data.List (lookup)
import Data.Set (toList, fromList)
import qualified Data.Text as T
import qualified SimpleSMT as S

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify.Context

-- TODO: Since S.SExpr is so similar, we could just drop this.
-- | A simple form of expressions featuring basic arithmetic.
data ConstraintExpr
  -- | Boolean value.
  = CBool !Bool
  -- | Reference to an expression variable.
  | CVar !ExprVarName
  -- | Sum of two expressions.
  | CAdd !ConstraintExpr !ConstraintExpr
  -- | Subtraction of two expressions.
  | CSub !ConstraintExpr !ConstraintExpr
  -- | Boolean and operator.
  | CAnd !ConstraintExpr !ConstraintExpr
  -- | Boolean not operator.
  | CNot !ConstraintExpr
  -- | If then else expression.
  | CIf !ConstraintExpr !ConstraintExpr !ConstraintExpr
  -- | If then expression.
  | CWhen !ConstraintExpr !ConstraintExpr

instance Show ConstraintExpr where
  show (CBool b) = show b
  show (CVar x) = T.unpack $ unExprVarName x
  show (CAdd e1 e2) = show e1 ++ " + " ++ show e2
  show (CSub e1 e2) = show e1 ++ " - " ++ show e2
  show (CAnd e1 e2) = show e1 ++ " and " ++ show e2
  show (CNot e) = "not " ++ show e
  show (CIf e1 e2 e3) = "if " ++ show e1 ++ " then " ++ show e2 ++ " else " ++ show e3
  show (CWhen e1 e2) = "when " ++ show e1 ++ " then " ++ show e2

-- | Class covering the types converteable to constraint expressions.
class ConstrExpr a where
  -- | Convert the given data type to a constraint expression.
  toCExp :: a -> ConstraintExpr

instance ConstrExpr BoolExpr where
  toCExp (BExpr e) = toCExp e
  toCExp (BAnd b1 b2) = CAnd (toCExp b1) (toCExp b2)
  toCExp (BNot b) = CNot (toCExp b)

instance ConstrExpr Expr where
  toCExp (EVar x) = CVar x
  toCExp (ERecProj _ f (EVar x)) = CVar $ recProj2Var x f
  toCExp (ETmApp (ETmApp (ETyApp (EBuiltin b) _) e1) e2) = case b of
    BEAddNumeric -> CAdd (toCExp e1) (toCExp e2)
    BESubNumeric -> CSub (toCExp e1) (toCExp e2)
    _ -> error ("Builtin: " ++ show b)
  toCExp (ELocation _ e) = toCExp e
  toCExp (EBuiltin (BEBool b)) = CBool b
  toCExp e = error ("Conversion: " ++ show e)

instance ConstrExpr a => ConstrExpr (Cond a) where
  toCExp (Determined x) = toCExp x
  toCExp (Conditional b x Nothing) = CWhen (toCExp b) (toCExp x)
  toCExp (Conditional b x (Just y)) = CIf (toCExp b) (toCExp x) (toCExp y)

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

-- | Filters a single update to match the given template, and takes out the
-- field of interest. The update gets converted into a constraint expression.
-- It returns either a create or an archive update.
filterUpd :: TypeConName
  -- ^ The template name to filter against.
  -> FieldName
  -- ^ The field name to be verified.
  -> Upd
  -- ^ The update expression to convert and filter.
  -> (Maybe ConstraintExpr, Maybe ConstraintExpr)
filterUpd tem f UpdCreate{..} = if tem == qualObject _creTemp
  then (Just (toCExp $ fromJust $ lookup f _creField), Nothing)
  else (Nothing, Nothing)
filterUpd tem f UpdArchive{..} = if tem == qualObject _arcTemp
  then (Nothing, Just (toCExp $ fromJust $ lookup f _arcField))
  else (Nothing, Nothing)

-- | Filters and converts a conditional update into (possibly two) constraint
-- expressions, while splitting it into create and archive updates.
filterCondUpd :: TypeConName
  -- ^ The template name to filter against
  -> FieldName
  -- ^ The field name to be verified.
  -> Cond Upd
  -- ^ The conditional update expression to convert and filter.
  -> ([ConstraintExpr], [ConstraintExpr])
filterCondUpd tem f (Determined x) = bimap maybeToList maybeToList $ filterUpd tem f x
filterCondUpd tem f (Conditional b x Nothing) =
  let cb = toCExp b
      cx = bimap maybeToList maybeToList $ filterUpd tem f x
  in bimap (map (CWhen cb)) (map (CWhen cb)) cx
filterCondUpd tem f (Conditional b x (Just y)) =
  let cb = toCExp b
      (cxcre,cxarc) = bimap maybeToList maybeToList $ filterUpd tem f x
      (cycre,cyarc) = bimap maybeToList maybeToList $ filterUpd tem f y
  -- TODO: We should try to use an if here.
  in ( map (CWhen cb) cxcre ++ map (CWhen (CNot cb)) cycre
     , map (CWhen cb) cxarc ++ map (CWhen (CNot cb)) cyarc )

-- | Constructs a constraint set from the generator environment, together with
-- the template name, the choice and field to be verified.
-- TODO: The choice and field don't actually need to be in the same template.
-- Take two templates as arguments.
constructConstr :: Env 'Solving
  -- ^ The generator environment to convert.
  -> TypeConName
  -- ^ The template name to be verified.
  -> ChoiceName
  -- ^ The choice name to be verified.
  -> FieldName
  -- ^ The field name to be verified.
  -> ConstraintSet
constructConstr env tem ch f =
  case lookupChoInHMap (_envschs env) tem ch of
    Just (self, this, arg, updSubst) ->
      let upds = _ussUpdate $ updSubst (EVar self) (EVar this) (EVar arg)
          vars = concatMap skol2var (_envsskol env)
          (cres, arcs) = foldl
            (\(cs,as) upd -> let (cs',as') = filterCondUpd tem f upd in (cs ++ cs',as ++ as'))
            ([],[]) upds
      in ConstraintSet vars cres arcs
    Nothing -> error "Choice not found"

-- | Convert a constraint expression into an SMT expression from the solving library.
cexp2sexp :: [(ExprVarName,S.SExpr)]
  -- ^ The set of variable names, mapped to their corresponding SMT counterparts.
  -> ConstraintExpr
  -- ^ The constraint expression to convert.
  -> IO S.SExpr
cexp2sexp _vars (CBool b) = return $ S.bool b
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
cexp2sexp vars (CAnd ce1 ce2) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  return $ S.and se1 se2
cexp2sexp vars (CNot ce) = do
  se <- cexp2sexp vars ce
  return $ S.not se
cexp2sexp vars (CIf ce1 ce2 ce3) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  se3 <- cexp2sexp vars ce3
  return $ S.ite se1 se2 se3
cexp2sexp vars (CWhen ce1 ce2) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  -- TODO: Temporary hack
  return $ S.ite se1 se2 (S.real 0)

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
