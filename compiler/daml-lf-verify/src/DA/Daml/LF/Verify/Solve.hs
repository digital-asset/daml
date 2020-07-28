-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}

-- | Constraint solver for DAML LF static verification
module DA.Daml.LF.Verify.Solve
  ( constructConstr
  , solveConstr
  , ConstraintSet(..)
  , Result(..)
  ) where

import Data.Bifunctor
import Data.Maybe
import Data.List
import Data.List.Extra (nubOrd)
import Data.Tuple.Extra (both)
import Data.Text.Prettyprint.Doc
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified SimpleSMT as S

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Numeric
import DA.Daml.LF.Verify.Context

-- TODO: Since S.SExpr is so similar, we could just drop this.
-- | A simple form of expressions featuring basic arithmetic.
data ConstraintExpr
  -- | Boolean value.
  = CBool !Bool
  -- | Integer value.
  | CInt !Integer
  -- | Real value.
  | CReal !Rational
  -- | Reference to an expression variable.
  | CVar !ExprVarName
  -- | Sum of two expressions.
  | CAdd !ConstraintExpr !ConstraintExpr
  -- | Subtraction of two expressions.
  | CSub !ConstraintExpr !ConstraintExpr
  -- | Equals operator.
  | CEq !ConstraintExpr !ConstraintExpr
  -- | Boolean and operator.
  | CAnd !ConstraintExpr !ConstraintExpr
  -- | Boolean not operator.
  | CNot !ConstraintExpr
  -- | If then else expression.
  | CIf !ConstraintExpr !ConstraintExpr !ConstraintExpr
  deriving Show

instance Pretty ConstraintExpr where
  pretty (CBool b) = pretty b
  pretty (CInt i) = pretty i
  pretty (CReal i) = pretty $ show i
  pretty (CVar x) = pretty $ unExprVarName x
  pretty (CAdd e1 e2) = pretty e1 <+> " <+> " <+> pretty e2
  pretty (CSub e1 e2) = pretty e1 <+> " - " <+> pretty e2
  pretty (CEq e1 e2) = pretty e1 <+> " == " <+> pretty e2
  pretty (CAnd e1 e2) = pretty e1 <+> " and " <+> pretty e2
  pretty (CNot e) = "not " <+> pretty e
  pretty (CIf e1 e2 e3) = "if " <+> pretty e1 <+> " then " <+> pretty e2
    <+> " else " <+> pretty e3

-- | Add a bunch of constraint expressions.
addMany :: [ConstraintExpr] -> ConstraintExpr
addMany [] = CReal 0.0
addMany [x] = x
addMany (x:xs) = CAdd x (addMany xs)

-- | Class covering the types convertible to constraint expressions.
class ConstrExpr a where
  -- | Convert the given data type to a constraint expression.
  toCExp :: [(ExprVarName, ExprVarName)]
    -- ^ The contract name synonyms, along with their current alias.
    -> a
    -- ^ The data to convert to a constraint expression.
    -> ConstraintExpr

instance ConstrExpr BoolExpr where
  toCExp syns (BExpr e) = toCExp syns e
  toCExp syns (BAnd b1 b2) = CAnd (toCExp syns b1) (toCExp syns b2)
  toCExp syns (BNot b) = CNot (toCExp syns b)

instance ConstrExpr Expr where
  toCExp syns (EVar x) = case lookup x syns of
    Just y -> CVar y
    Nothing -> CVar x
  toCExp syns (ERecProj _ f (EVar x)) = case lookup x syns of
    Just y -> CVar $ recProj2Var y f
    Nothing -> CVar $ recProj2Var x f
  toCExp syns (EStructProj f (EVar x)) = case lookup x syns of
    Just y -> CVar $ recProj2Var y f
    Nothing -> CVar $ recProj2Var x f
  toCExp syns (ETmApp (ETmApp op e1) e2) = case op of
    (EBuiltin (BEEqual _)) -> CEq (toCExp syns e1) (toCExp syns e2)
    (EBuiltin BEAddInt64) -> CAdd (toCExp syns e1) (toCExp syns e2)
    (EBuiltin BESubInt64) -> CSub (toCExp syns e1) (toCExp syns e2)
    (ETyApp (EBuiltin BEAddNumeric) _) -> CAdd (toCExp syns e1) (toCExp syns e2)
    (ETyApp (EBuiltin BESubNumeric) _) -> CSub (toCExp syns e1) (toCExp syns e2)
    (ETmApp (ETyApp (EVal (Qualified _ _ (ExprValName "+"))) _) _) ->
      CAdd (toCExp syns e1) (toCExp syns e2)
    (ETmApp (ETyApp (EVal (Qualified _ _ (ExprValName "-"))) _) _) ->
      CSub (toCExp syns e1) (toCExp syns e2)
    _ -> error ("Builtin: " ++ show op)
  toCExp syns (ELocation _ e) = toCExp syns e
  toCExp _syns (EBuiltin (BEBool b)) = CBool b
  toCExp _syns (EBuiltin (BEInt64 i)) = CInt $ toInteger i
  toCExp _syns (EBuiltin (BENumeric i)) = CReal $ toRational $ numericDecimal i
  toCExp _syns e = error ("Conversion: " ++ show e)

instance ConstrExpr a => ConstrExpr (Cond a) where
  toCExp syns (Determined x) = toCExp syns x
  -- TODO: Can we assume this should always be a sum?
  toCExp syns (Conditional b x y) = CIf (toCExp syns b)
    (addMany $ map (toCExp syns) x)
    (addMany $ map (toCExp syns) y)

-- | Gather all free variables in a constraint expression.
gatherFreeVars :: ConstraintExpr
  -- ^ The constraint expression to traverse.
  -> [ExprVarName]
gatherFreeVars (CBool _) = []
gatherFreeVars (CInt _) = []
gatherFreeVars (CReal _) = []
gatherFreeVars (CVar x) = [x]
gatherFreeVars (CAdd e1 e2) = gatherFreeVars e1 `union` gatherFreeVars e2
gatherFreeVars (CSub e1 e2) = gatherFreeVars e1 `union` gatherFreeVars e2
gatherFreeVars (CEq e1 e2) = gatherFreeVars e1 `union` gatherFreeVars e2
gatherFreeVars (CAnd e1 e2) = gatherFreeVars e1 `union` gatherFreeVars e2
gatherFreeVars (CNot e) = gatherFreeVars e
gatherFreeVars (CIf e1 e2 e3) = gatherFreeVars e1 `union`
  gatherFreeVars e2 `union` gatherFreeVars e3

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
    -- ^ The field values of all newly created contracts.
  , _cArcs :: ![ConstraintExpr]
    -- ^ The field values of all archived contracts.
  , _cCtrs :: ![(ConstraintExpr, ConstraintExpr)]
    -- ^ Additional equality constraints.
  }
  deriving Show

-- | Filters a single update to match the given template, and takes out the
-- field of interest. The update gets converted into a constraint expression.
-- It returns either a create or an archive update.
filterUpd :: Qualified TypeConName
  -- ^ The template name to filter against.
  -> [(ExprVarName, ExprVarName)]
  -- ^ The contract name synonyms, along with their current alias.
  -> FieldName
  -- ^ The field name to be verified.
  -> Upd
  -- ^ The update expression to convert and filter.
  -> (Maybe ConstraintExpr, Maybe ConstraintExpr)
filterUpd tem syns f UpdCreate{..} = if tem == _creTemp
  then (Just (toCExp syns $ fromJust $ lookup f _creField), Nothing)
  else (Nothing, Nothing)
filterUpd tem syns f UpdArchive{..} = if tem == _arcTemp
  then (Nothing, Just (toCExp syns $ fromJust $ lookup f _arcField))
  else (Nothing, Nothing)

-- | Filters and converts a conditional update into (possibly two) constraint
-- expressions, while splitting it into create and archive updates.
filterCondUpd :: Qualified TypeConName
  -- ^ The template name to filter against
  -> [(ExprVarName, ExprVarName)]
  -- ^ The contract name synonyms, along with their current alias.
  -> FieldName
  -- ^ The field name to be verified.
  -> Cond Upd
  -- ^ The conditional update expression to convert and filter.
  -> ([ConstraintExpr], [ConstraintExpr])
filterCondUpd tem syns f (Determined x) = both maybeToList $ filterUpd tem syns f x
filterCondUpd tem syns f (Conditional b xs ys) =
  let cb = toCExp syns b
      (cxcre,cxarc) = both (addMany . concat) $ unzip $ map (filterCondUpd tem syns f) xs
      (cycre,cyarc) = both (addMany . concat) $ unzip $ map (filterCondUpd tem syns f) ys
  in ( [CIf cb cxcre cycre]
     , [CIf cb cxarc cyarc] )

-- | Filter the given set of skolems, to only include those that occur in the
-- given constraint expressions. Remove duplicates in the process.
filterVars :: [ExprVarName]
  -- ^ The list of skolems to filter.
  -> [ConstraintExpr]
  -- ^ The constraint expressions in which the skolems should occur.
  -> [ExprVarName]
filterVars vars cexprs =
  let freevars = foldl' (\fv e -> fv `union` gatherFreeVars e) [] cexprs
  in freevars `intersect` vars

-- | Construct a list of all contract name synonyms, along with their current
-- alias.
constructSynonyms :: [(ExprVarName, [ExprVarName])]
  -- ^ The current contract names, along with any previous synonyms.
  -> [(ExprVarName, ExprVarName)]
constructSynonyms = foldl step []
  where
    step :: [(ExprVarName, ExprVarName)] -> (ExprVarName, [ExprVarName])
      -> [(ExprVarName, ExprVarName)]
    step acc (cur, prevs) = acc ++ map (, cur) prevs

-- | Constructs a constraint set from the generator environment, together with
-- the template name, the choice and field to be verified.
constructConstr :: Env 'Solving
  -- ^ The generator environment to convert.
  -> Qualified TypeConName
  -- ^ The template name of the choice to be verified.
  -> ChoiceName
  -- ^ The choice name to be verified.
  -> Qualified TypeConName
  -- ^ The template name of the field to be verified.
  -> FieldName
  -- ^ The field name to be verified.
  -> ConstraintSet
constructConstr env chtem ch ftem f =
  case HM.lookup (UpdChoice chtem ch) (_envschs env) of
    Just ChoiceData{..} ->
      let upds = _ussUpdate $ _cdUpds (EVar _cdSelf) (EVar _cdThis) (EVar _cdArgs)
          vars = concatMap skol2var (_envsskol env)
          syns = constructSynonyms $ HM.elems $ _envscids env
          ctrs = map (both (toCExp syns)) $ _envsctrs env
          (cres, arcs) = foldl
            (\(cs,as) upd -> let (cs',as') = filterCondUpd ftem syns f upd in (cs ++ cs',as ++ as'))
            ([],[]) upds
      in ConstraintSet vars cres arcs ctrs
    Nothing -> error ("Choice not found " ++ show ch)

-- | Convert a constraint expression into an SMT expression from the solving library.
cexp2sexp :: [(ExprVarName,S.SExpr)]
  -- ^ The set of variable names, mapped to their corresponding SMT counterparts.
  -> ConstraintExpr
  -- ^ The constraint expression to convert.
  -> IO S.SExpr
cexp2sexp _vars (CBool b) = return $ S.bool b
cexp2sexp _vars (CInt i) = return $ S.int i
cexp2sexp _vars (CReal i) = return $ S.real i
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
cexp2sexp vars (CEq ce1 ce2) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  return $ S.eq se1 se2
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

-- | Assert the additional equality constraints. Binds and returns any
-- additional required variables.
declareCtrs :: S.Solver
  -- ^ The SMT solver.
  -> (String -> IO ())
  -- ^ Function for debugging printouts.
  -> [(ExprVarName,S.SExpr)]
  -- ^ The set of variable names, mapped to their corresponding SMT counterparts.
  -> [(ConstraintExpr, ConstraintExpr)]
  -- ^ The equality constraints to be declared.
  -> IO [(ExprVarName,S.SExpr)]
declareCtrs sol debug cvars1 ctrs = do
  let edges = map (\(l,r) -> (l,r,gatherFreeVars l ++ gatherFreeVars r)) ctrs
      components = conn_comp edges
      useful_nodes = map fst cvars1
      useful_components = filter
        (\comp -> let comp_vars = concatMap (\(_,_,vars) -> vars) comp
                  in not $ null $ intersect comp_vars useful_nodes)
        components
      useful_equalities = concatMap (map (\(l,r,_) -> (l,r))) useful_components
      required_vars =
        nubOrd (concatMap (concatMap (\(_,_,vars) -> vars)) useful_components)
        \\ useful_nodes
  cvars2 <- declareVars sol required_vars
  mapM_ (declare $ cvars1 ++ cvars2) useful_equalities
  return cvars2
  where
    -- | Compute connected components of the equality constraints graph.
    -- Two edges are adjacent when at least one of their nodes shares a variable.
    conn_comp :: [(ConstraintExpr,ConstraintExpr,[ExprVarName])]
      -- ^ The edges of the graph, annotated with the contained variables.
      -> [[(ConstraintExpr,ConstraintExpr,[ExprVarName])]]
    conn_comp [] = []
    conn_comp (edge:edges) = let (comp,rem) = cc_step edges edge
      in comp : conn_comp rem

    -- | Compute the strongly connected component containing a given edge from
    -- the graph, as well as the remaining edges which do not belong to this
    -- component.
    cc_step :: [(ConstraintExpr,ConstraintExpr,[ExprVarName])]
      -- ^ The edges of the graph, which do not yet belong to any component.
      -> (ConstraintExpr,ConstraintExpr,[ExprVarName])
      -- ^ The current edge for which the component is being computed.
      -> ( [(ConstraintExpr,ConstraintExpr,[ExprVarName])]
         -- ^ The computed connected component.
         , [(ConstraintExpr,ConstraintExpr,[ExprVarName])] )
         -- ^ The remaining edges which do not belong to the connected component.
    cc_step [] _ = ([],[])
    cc_step edges0 (l,r,vars) =
      let (neighbors,edges1) = partition (\(_,_,vars') -> not $ null $ intersect vars vars') edges0
      in foldl (\(conn,edges2) edge -> first (conn ++) $ cc_step edges2 edge)
           ((l,r,vars):neighbors,edges1) neighbors

    declare :: [(ExprVarName,S.SExpr)] -> (ConstraintExpr, ConstraintExpr) -> IO ()
    declare vars (cexp1, cexp2) = do
      sexp1 <- cexp2sexp vars cexp1
      sexp2 <- cexp2sexp vars cexp2
      debug ("Assert: " ++ S.ppSExpr sexp1 (" = " ++ S.ppSExpr sexp2 ""))
      S.assert sol (sexp1 `S.eq` sexp2)

-- | Data type denoting the outcome of the solver.
data Result
  = Success
  -- ^ The total field amount remains preserved.
  | Fail [(S.SExpr, S.Value)]
  -- ^ The total field amound does not remain the same. A counter example is
  -- provided.
  | Unknown
  -- ^ The result is inconclusive.
  deriving Eq

instance Show Result where
  show Success = "Success!"
  show (Fail cs) = "Fail. Counter example:" ++ foldl (flip step) "" cs
    where
      step :: (S.SExpr, S.Value) -> String -> String
      step (var, val) str = ("\n" ++) $ S.ppSExpr var $ (" = " ++) $ S.ppSExpr (S.value val) str
  show Unknown = "Inconclusive."

-- | Solve a give constraint set. Prints 'unsat' when the constraint set is
-- valid. It asserts that the set of created and archived contracts are not
-- equal.
solveConstr :: FilePath
  -- ^ The path to the constraint solver.
  -> (String -> IO ())
  -- ^ Function for debugging printouts.
  -> ConstraintSet
  -- ^ The constraint set to solve.
  -> IO Result
solveConstr spath debug ConstraintSet{..} = do
  log <- S.newLogger 1
  sol <- S.newSolver spath ["-in"] (Just log)
  vars1 <- declareVars sol $ filterVars _cVars (_cCres ++ _cArcs)
  vars2 <- declareCtrs sol debug vars1 _cCtrs
  let vars = vars1 ++ vars2
  cre <- foldl S.add (S.real 0.0) <$> mapM (cexp2sexp vars) _cCres
  arc <- foldl S.add (S.real 0.0) <$> mapM (cexp2sexp vars) _cArcs
  S.assert sol (S.not (cre `S.eq` arc))
  S.check sol >>= \case
    S.Sat -> do
      counter <- S.getExprs sol $ map snd vars
      return $ Fail counter
    S.Unsat -> return Success
    S.Unknown -> return Unknown
