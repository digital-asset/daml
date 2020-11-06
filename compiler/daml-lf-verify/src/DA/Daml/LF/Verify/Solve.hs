-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}

-- | Constraint solver for DAML LF static verification
module DA.Daml.LF.Verify.Solve
  ( constructConstr
  , solveConstr
  , ConstraintSet(..)
  , Result(..)
  , showResult
  ) where

import Data.Bifunctor
import Data.Maybe
import Data.List
import Data.List.Extra (nubOrd)
import Data.Tuple.Extra (both)
import Data.Text.Prettyprint.Doc
import Data.Text.Prettyprint.Doc.Render.String
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified SimpleSMT as S

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Numeric
import DA.Daml.LF.Verify.Context

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
  -- | Multiplication of two expressions.
  | CMul !ConstraintExpr !ConstraintExpr
  -- | Division of two expressions.
  | CDiv !ConstraintExpr !ConstraintExpr
  -- | Negation of an expression.
  | CNeg !ConstraintExpr
  -- | Modulo operation on expressions.
  | CMod !ConstraintExpr !ConstraintExpr
  -- | Boolean operator.
  | COp !CtrOperator !ConstraintExpr !ConstraintExpr
  -- | Boolean and operator.
  | CAnd !ConstraintExpr !ConstraintExpr
  -- | Boolean not operator.
  | CNot !ConstraintExpr
  -- | If then else expression.
  | CIf !ConstraintExpr !ConstraintExpr !ConstraintExpr
  deriving (Eq, Ord, Show)

-- | Binary boolean operator for constraint expressions.
data CtrOperator
  -- | Equals operator.
  = OpEq
  -- | Greater than operator.
  | OpGt
  -- | Greater than or equal operator.
  | OpGtE
  -- | Less than operator.
  | OpLt
  -- | Less than or equal operator.
  | OpLtE
  deriving (Eq, Ord, Show)

instance Pretty ConstraintExpr where
  pretty (CBool b) = pretty b
  pretty (CInt i) = pretty i
  pretty (CReal i) = pretty $ show i
  pretty (CVar x) = pretty $ unExprVarName x
  pretty (CAdd e1 e2) = pretty e1 <+> " + " <+> pretty e2
  pretty (CSub e1 e2) = pretty e1 <+> " - " <+> pretty e2
  pretty (CMul e1 e2) = parens (pretty e1) <+> " * " <+> parens (pretty e2)
  pretty (CDiv e1 e2) = parens (pretty e1) <+> " / " <+> parens (pretty e2)
  pretty (CNeg e) = " -" <+> parens (pretty e)
  pretty (CMod e1 e2) = parens (pretty e1) <+> " % " <+> parens (pretty e2)
  pretty (COp op e1 e2) = pretty e1 <+> pretty op <+> pretty e2
  pretty (CAnd e1 e2) = pretty e1 <+> " and " <+> pretty e2
  pretty (CNot e) = "not " <+> pretty e
  pretty (CIf e1 e2 e3) = "if " <+> pretty e1 <+> " then " <+> pretty e2
    <+> " else " <+> pretty e3

instance Pretty CtrOperator where
  pretty OpEq = "="
  pretty OpGt = ">"
  pretty OpGtE = ">="
  pretty OpLt = "<"
  pretty OpLtE = "<="

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
  toCExp syns (BEq b1 b2) = COp OpEq (toCExp syns b1) (toCExp syns b2)
  toCExp syns (BGt b1 b2) = COp OpGt (toCExp syns b1) (toCExp syns b2)
  toCExp syns (BGtE b1 b2) = COp OpGtE (toCExp syns b1) (toCExp syns b2)
  toCExp syns (BLt b1 b2) = COp OpLt (toCExp syns b1) (toCExp syns b2)
  toCExp syns (BLtE b1 b2) = COp OpLtE (toCExp syns b1) (toCExp syns b2)

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
  toCExp syns (ETmApp (ETmApp op e1) e2) =
    builtin_op op (toCExp syns e1) (toCExp syns e2)
    where
      builtin_op :: Expr -> ConstraintExpr -> ConstraintExpr -> ConstraintExpr
      builtin_op (ETmApp (ETyApp (EVal (Qualified _ _ (ExprValName w))) _) _) = case w of
        "+" -> CAdd
        "-" -> CSub
        _ -> error ("Unsupported builtin value: " ++ T.unpack w)
      builtin_op (EVal (Qualified _ _ (ExprValName w))) = case w of
        "negate" -> const CNeg
        _ -> error ("Unsupported builtin value: " ++ T.unpack w)
      builtin_op (ETyApp e _) = builtin_op e
      builtin_op (EBuiltin op) = case op of
        BEEqual _ -> COp OpEq
        BEEqualNumeric -> COp OpEq
        BEGreater _ -> COp OpGt
        BEGreaterNumeric -> COp OpGt
        BEGreaterEq _ -> COp OpGtE
        BEGreaterEqNumeric -> COp OpGtE
        BELess _ -> COp OpLt
        BELessNumeric -> COp OpLt
        BELessEq _ -> COp OpLtE
        BELessEqNumeric -> COp OpLtE
        BEAddInt64 -> CAdd
        BEAddNumeric -> CAdd
        BEAddDecimal -> CAdd
        BESubInt64 -> CSub
        BESubNumeric -> CSub
        BESubDecimal -> CSub
        BEMulInt64 -> CMul
        BEMulNumeric -> CMul
        BEMulDecimal -> CMul
        BEDivInt64 -> CDiv
        BEDivNumeric -> CDiv
        BEDivDecimal -> CDiv
        BEModInt64 -> CMod
        _ -> error ("Unsupported builtin operator: " ++ show op)
      builtin_op e = error ("Unsupported builtin expression: " ++ show e)
  toCExp syns (ETmApp (ETyApp (EVal (Qualified _ _ (ExprValName w))) _) e) = case w of
    "intToNumeric" -> toCExp syns e
    "$cnegate1" -> CNeg (toCExp syns e)
    _ -> error ("Unsupported builtin value: " ++ T.unpack w)
  toCExp syns (ETmApp (ETyApp (ETyApp (EBuiltin BECastNumeric) _) _) e) = toCExp syns e
  toCExp syns (ELocation _ e) = toCExp syns e
  toCExp _syns (EBuiltin (BEBool b)) = CBool b
  toCExp _syns (EBuiltin (BEInt64 i)) = CInt $ toInteger i
  toCExp _syns (EBuiltin (BENumeric i)) = CReal $ toRational $ numericDecimal i
  toCExp syns (ERecProj _ f (ERecCon _ fields)) = toCExp syns $ fromJust $ lookup f fields
  toCExp _syns e = error ("Conversion: " ++ show e)

instance ConstrExpr a => ConstrExpr (Cond a) where
  toCExp syns (Determined x) = toCExp syns x
  -- TODO: Simplifications could be added here later on.
  -- e.g. Remove the conditional when both sides are equal.
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
gatherFreeVars (CMul e1 e2) = gatherFreeVars e1 `union` gatherFreeVars e2
gatherFreeVars (CDiv e1 e2) = gatherFreeVars e1 `union` gatherFreeVars e2
gatherFreeVars (CNeg e) = gatherFreeVars e
gatherFreeVars (CMod e1 e2) = gatherFreeVars e1 `union` gatherFreeVars e2
gatherFreeVars (COp _ e1 e2) = gatherFreeVars e1 `union` gatherFreeVars e2
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
  , _cCtrs :: ![ConstraintExpr]
    -- ^ Additional boolean constraints.
  , _cInfo :: !String
    -- ^ Debugging information.
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
  -> BaseUpd
  -- ^ The update expression to convert and filter.
  -> (Maybe ConstraintExpr, Maybe ConstraintExpr)
filterUpd tem syns f UpdCreate{..} = if tem == _creTemp
  then (Just (toCExp syns $ fromJust $ lookup f _creField), Nothing)
  else (Nothing, Nothing)
filterUpd tem syns f UpdArchive{..} = if tem == _arcTemp
  then (Nothing, Just (toCExp syns $ fromJust $ lookup f _arcField))
  else (Nothing, Nothing)

-- | Filters and converts a conditional update into constraint
-- expressions, while splitting it into create and archive updates.
filterCondUpd :: Qualified TypeConName
  -- ^ The template name to filter against
  -> [(ExprVarName, ExprVarName)]
  -- ^ The contract name synonyms, along with their current alias.
  -> FieldName
  -- ^ The field name to be verified.
  -> Cond BaseUpd
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
constructSynonyms = foldl' step []
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
  -> [ConstraintSet]
constructConstr env chtem ch ftem f =
  case HM.lookup (UpdChoice chtem ch) (envChoices env) of
    Just ChoiceData{..} ->
      let upds = [upd | UpdSBase upd <- _cdUpds]
          vars = concatMap skol2var (envSkols env)
          syns = constructSynonyms $ HM.elems $ envCids env
          ctrs = map (toCExp syns) (envCtrs env)
          cycleUpds = computeCycles upds
      in map (constructSingleSet vars ctrs syns) cycleUpds
    Nothing -> error ("Choice not found " ++ show ch)
  where
    constructSingleSet :: [ExprVarName]
      -- ^ The variables to be declared.
      -> [ConstraintExpr]
      -- ^ The additional constraints.
      -> [(ExprVarName, ExprVarName)]
      -- ^ The contract name synonyms, along with their current alias.
      -> (String, [Cond BaseUpd])
      -- ^ The updates to analyse.
      -> ConstraintSet
    constructSingleSet vars ctrs syns (info, upds) =
      let (cres, arcs) = foldl'
            (\(cs,as) upd -> let (cs',as') = filterCondUpd ftem syns f upd in (cs ++ cs',as ++ as'))
            ([],[])
            upds
      in ConstraintSet vars cres arcs ctrs info

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
cexp2sexp vars (CMul ce1 ce2) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  return $ S.mul se1 se2
cexp2sexp vars (CDiv ce1 ce2) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  return $ S.realDiv se1 se2
cexp2sexp vars (CNeg ce) = do
  se <- cexp2sexp vars ce
  return $ S.neg se
cexp2sexp vars (CMod ce1 ce2) = do
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  return $ S.mod se1 se2
cexp2sexp vars (COp cop ce1 ce2) = do
  let sop = case cop of
        OpEq -> S.eq
        OpGt -> S.gt
        OpGtE -> S.geq
        OpLt -> S.lt
        OpLtE -> S.leq
  se1 <- cexp2sexp vars ce1
  se2 <- cexp2sexp vars ce2
  return $ sop se1 se2
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

-- | Perform some basic simplifications on constraint expressions.
simplifyCExpr :: ConstraintExpr -> ConstraintExpr
simplifyCExpr (CNot e) = case simplifyCExpr e of
  CNot e' -> e'
  e' -> CNot e'
simplifyCExpr (CAnd e1 e2) =
  let e1' = simplifyCExpr e1
      e2' = simplifyCExpr e2
  in if e1' == e2' then e1'
     else if e1' == CNot e2' || CNot e1' == e2' then CBool False
     else case e2' of
       CAnd e3 e4 ->
         if e3 == e1' || e4 == e1' then e2'
         else if e3 == CNot e1' || e4 == CNot e1' || CNot e3 == e1' || CNot e4 == e1'
              then CBool False
              else CAnd e1' e2'
       _ -> CAnd e1' e2'
simplifyCExpr (CIf e1 e2 e3) = case simplifyCExpr e1 of
  CBool True -> e2
  CBool False -> e3
  e1' ->
    let e2' = simplifyCExpr e2
        e3' = simplifyCExpr e3
    in if e2' == e3'
       then e2'
       else CIf e1' e2' e3'
simplifyCExpr (CAdd e1 e2) = CAdd (simplifyCExpr e1) (simplifyCExpr e2)
simplifyCExpr (CSub e1 e2) = CSub (simplifyCExpr e1) (simplifyCExpr e2)
simplifyCExpr (CMul e1 e2) = CMul (simplifyCExpr e1) (simplifyCExpr e2)
simplifyCExpr (CDiv e1 e2) = CDiv (simplifyCExpr e1) (simplifyCExpr e2)
simplifyCExpr (CNeg e) = CNeg (simplifyCExpr e)
simplifyCExpr (CMod e1 e2) = CMod (simplifyCExpr e1) (simplifyCExpr e2)
simplifyCExpr (COp op e1 e2) = COp op (simplifyCExpr e1) (simplifyCExpr e2)
-- TODO: This could be extended with additional cases later on.
simplifyCExpr e = e

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
  -> [(ExprVarName,S.SExpr)]
  -- ^ The set of variable names, mapped to their corresponding SMT counterparts.
  -> [ConstraintExpr]
  -- ^ The constraints to be declared.
  -> IO (String, [(ExprVarName,S.SExpr)])
declareCtrs sol cvars1 exprs = do
  let edges = map toTuple exprs
  -- let edges = map (\(l,r) -> (l,r,gatherFreeVars l ++ gatherFreeVars r)) ctrs
      components = conn_comp edges
      useful_nodes = map fst cvars1
      useful_components = filter
        (\comp -> let comp_vars = concatMap (\(_,_,_,vars) -> vars) comp
                  in not $ null $ intersect comp_vars useful_nodes)
        components
      useful_equalities = concatMap (map (\(o,l,r,_) -> (o,l,r))) useful_components
      required_vars =
        nubOrd (concatMap (concatMap (\(_,_,_,vars) -> vars)) useful_components)
        \\ useful_nodes
  cvars2 <- declareVars sol required_vars
  debug <- intercalate "\n" <$> mapM (declare $ cvars1 ++ cvars2) useful_equalities
  return (debug, cvars2)
  where
    -- | Convert the constraint expression into a tuple of a binary operator, a
    -- left and right expression, and the enclosed variables.
    toTuple :: ConstraintExpr
      -- ^ The expression to convert.
      -> (CtrOperator, ConstraintExpr, ConstraintExpr, [ExprVarName])
    toTuple e = case e of
      (COp op cexpr1 cexpr2) -> (op, cexpr1, cexpr2, gatherFreeVars e)
      _ -> error ("Invalid constraint expression: " ++ show e)

    -- | Compute connected components of the equality constraints graph.
    -- Two edges are adjacent when at least one of their nodes shares a variable.
    conn_comp :: [(CtrOperator,ConstraintExpr,ConstraintExpr,[ExprVarName])]
      -- ^ The edges of the graph, annotated with the contained variables.
      -> [[(CtrOperator,ConstraintExpr,ConstraintExpr,[ExprVarName])]]
    conn_comp [] = []
    conn_comp (edge:edges) = let (comp,rem) = cc_step edges edge
      in comp : conn_comp rem

    -- | Compute the strongly connected component containing a given edge from
    -- the graph, as well as the remaining edges which do not belong to this
    -- component.
    cc_step :: [(CtrOperator,ConstraintExpr,ConstraintExpr,[ExprVarName])]
      -- ^ The edges of the graph, which do not yet belong to any component.
      -> (CtrOperator,ConstraintExpr,ConstraintExpr,[ExprVarName])
      -- ^ The current edge for which the component is being computed.
      -> ( [(CtrOperator,ConstraintExpr,ConstraintExpr,[ExprVarName])]
         -- ^ The computed connected component.
         , [(CtrOperator,ConstraintExpr,ConstraintExpr,[ExprVarName])] )
         -- ^ The remaining edges which do not belong to the connected component.
    cc_step [] _ = ([],[])
    cc_step edges0 (o,l,r,vars) =
      let (neighbors,edges1) = partition (\(_,_,_,vars') -> not $ null $ intersect vars vars') edges0
      in foldl' (\(conn,edges2) edge -> first (conn ++) $ cc_step edges2 edge)
           ((o,l,r,vars):neighbors,edges1) neighbors

    declare :: [(ExprVarName,S.SExpr)]
      -> (CtrOperator, ConstraintExpr, ConstraintExpr)
      -> IO String
    declare vars (op, cexp1, cexp2) = do
      sexp1 <- cexp2sexp vars cexp1
      sexp2 <- cexp2sexp vars cexp2
      case op of
        OpEq -> do
          S.assert sol (sexp1 `S.eq` sexp2)
          return ("Assert: " ++ S.ppSExpr sexp1 (" = " ++ S.ppSExpr sexp2 ""))
        OpGt -> do
          S.assert sol (sexp1 `S.gt` sexp2)
          return ("Assert: " ++ S.ppSExpr sexp1 (" > " ++ S.ppSExpr sexp2 ""))
        OpGtE -> do
          S.assert sol (sexp1 `S.geq` sexp2)
          return ("Assert: " ++ S.ppSExpr sexp1 (" >= " ++ S.ppSExpr sexp2 ""))
        OpLt -> do
          S.assert sol (sexp1 `S.lt` sexp2)
          return ("Assert: " ++ S.ppSExpr sexp1 (" < " ++ S.ppSExpr sexp2 ""))
        OpLtE -> do
          S.assert sol (sexp1 `S.leq` sexp2)
          return ("Assert: " ++ S.ppSExpr sexp1 (" <= " ++ S.ppSExpr sexp2 ""))

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
  show (Fail cs) = "Fail. Counter example:" ++ foldl' (flip step) "" cs
    where
      step :: (S.SExpr, S.Value) -> String -> String
      step (var, val) str = ("\n" ++) $ S.ppSExpr var $ (" = " ++) $ S.ppSExpr (S.value val) str
  show Unknown = "Inconclusive."

-- | Output a result to a String, including the choice and field names.
showResult :: ChoiceName -> FieldName -> Result -> String
showResult choice field result = case result of
  Success -> "Success! The choice " ++ choiceStr ++ " preserves the field "
    ++ fieldStr ++ "."
  (Fail cs) -> "Fail. The choice " ++ choiceStr ++ " does not preserve the field "
    ++ fieldStr ++ ". Counter example:" ++ foldl' (flip step) "" cs
  Unknown -> "Inconclusive result."
  where
    choiceStr = T.unpack $ unChoiceName choice
    fieldStr = T.unpack $ unFieldName field
    step :: (S.SExpr, S.Value) -> String -> String
    step (var, val) str = ("\n" ++) $ S.ppSExpr var $ (" = " ++) $ S.ppSExpr (S.value val) str

-- | Solve a give constraint set. Prints 'unsat' when the constraint set is
-- valid. It asserts that the set of created and archived contracts are not
-- equal. Returns both the debugging information, and the result.
solveConstr :: FilePath
  -- ^ The path to the constraint solver.
  -> ConstraintSet
  -- ^ The constraint set to solve.
  -> IO (ChoiceName -> FieldName -> String, Result)
solveConstr spath ConstraintSet{..} = do
  log <- S.newLogger 1
  sol <- S.newSolver spath ["-in"] (Just log)
  vars1 <- declareVars sol $ filterVars _cVars (_cCres ++ _cArcs)
  let ctrs = nubOrd $ map simplifyCExpr _cCtrs
  (ctr_debug, vars2) <- declareCtrs sol vars1 ctrs
  vars <- if null (vars1 ++ vars2)
    then declareVars sol [ExprVarName "var"]
    else pure (vars1 ++ vars2)
  let cres = renderFilter $ map simplifyCExpr _cCres
      arcs = renderFilter $ map simplifyCExpr _cArcs
  cre <- foldl' S.add (S.real 0.0) <$> mapM (cexp2sexp vars) cres
  arc <- foldl' S.add (S.real 0.0) <$> mapM (cexp2sexp vars) arcs
  S.assert sol (S.not (cre `S.eq` arc))
  result <- S.check sol >>= \case
    S.Sat -> do
      counter <- S.getExprs sol $ map snd vars
      return $ Fail counter
    S.Unsat -> return Success
    S.Unknown -> return Unknown
  let debug (ch :: ChoiceName) (f :: FieldName) =
           "\n==========\n\n"
        ++ _cInfo ++ "\n\n"
        ++ (renderString $ layoutCompact ("Create: " <+> pretty cres <+> "\n"))
        ++ (renderString $ layoutCompact ("Archive: " <+> pretty arcs <+> "\n"))
        ++ ctr_debug
        ++ "\n~~~~~~~~~~\n\n"
        ++ showResult ch f result
  return (debug, result)

-- | Remove some redundant values from the list, to unclutter the render.
renderFilter :: [ConstraintExpr] -> [ConstraintExpr]
renderFilter = filter (\c -> c /= CInt 0 && c /= CReal 0.0)
