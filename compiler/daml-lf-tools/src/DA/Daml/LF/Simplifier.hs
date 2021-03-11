-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Simplifier(
    freeVarsStep,
    simplifyModule,
    ) where

import Control.Monad (guard, forM, forM_)
import Control.Monad.State.Strict (State, evalState, gets, modify)
import Data.Maybe (mapMaybe)
import Data.Foldable (fold, toList)
import Data.Functor.Foldable (cata, embed)
import qualified Data.Graph as G
import qualified Data.Text as T
import qualified Data.Set as Set
import qualified Data.Map.Strict as Map
import qualified Data.NameMap as NM
import qualified Safe
import qualified Safe.Exact as Safe

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Subst
import DA.Daml.LF.Ast.Recursive
import DA.Daml.LF.Ast.FreeVars
import DA.Daml.LF.Ast.Optics
import DA.Daml.LF.TypeChecker.Check
import DA.Daml.LF.TypeChecker.Env

-- | Models an approximation of the error safety of an expression. 'Unsafe'
-- means the expression might throw an error. @'Safe' /n/@ means that the
-- expression won't throw an error unless you apply it to at least /n/
-- arguments.
data Safety
  = Unsafe
  | Safe Int  -- number is >= 0
  deriving (Eq, Ord)

-- | This is used to track the necessary information for typeclass dictionary
-- inlining and projecion. We really only want to inline typeclass projection
-- function applied to dictionary functions, so we need to keep track of what
-- is what.
--
-- We rely on laziness to prevent actual inlining/substitution until it is
-- confirmed to be necessary.
data TypeClassInfo
  = TCNeither
  | TCProjection Expr
  | TCDictionary Expr

data Info = Info
  { freeVars :: FreeVars
  , safety   :: Safety
  , tcinfo   :: TypeClassInfo
  }

decrSafety :: Safety -> Safety
decrSafety = \case
  Unsafe        -> Unsafe
  Safe n
    | n > 0     -> Safe (n-1)
    | otherwise -> Unsafe

incrSafety :: Safety -> Safety
incrSafety = \case
  Unsafe -> Safe 0
  Safe n -> Safe (n+1)

-- | @'cata' safetyStep@ approximates the safety of an expression.
safetyStep :: ExprF Safety -> Safety
safetyStep = \case
  EVarF _ -> Safe 0
  EValF _ -> Unsafe
  EBuiltinF b ->
    case b of
      BEInt64 _           -> Safe 0
      BEDecimal _         -> Safe 0
      BENumeric _         -> Safe 0
      BEText _            -> Safe 0
      BETimestamp _       -> Safe 0
      BEParty _           -> Safe 0
      BEDate _            -> Safe 0
      BEUnit              -> Safe 0
      BEBool _            -> Safe 0
      BEError             -> Safe 0
      BEAnyExceptionMessage -> Safe 1
      BEGeneralErrorMessage -> Safe 1
      BEArithmeticErrorMessage -> Safe 1
      BEContractErrorMessage -> Safe 1
      BEMakeGeneralError -> Safe 1
      BEMakeArithmeticError -> Safe 1
      BEMakeContractError -> Safe 1
      BEEqualGeneric      -> Safe 1 -- may crash if values are incomparable
      BELessGeneric       -> Safe 1 -- may crash if values are incomparable
      BELessEqGeneric     -> Safe 1 -- may crash if values are incomparable
      BEGreaterGeneric    -> Safe 1 -- may crash if values are incomparable
      BEGreaterEqGeneric  -> Safe 1 -- may crash if values are incomparable
      BEEqual _           -> Safe 2
      BELess _            -> Safe 2
      BELessEq _          -> Safe 2
      BEGreaterEq _       -> Safe 2
      BEGreater _         -> Safe 2
      BEToText _          -> Safe 1
      BEToTextContractId  -> Safe 1
      BETextFromCodePoints  -> Safe 1
      BEAddDecimal        -> Safe 1
      BESubDecimal        -> Safe 1
      BEMulDecimal        -> Safe 1
      BEDivDecimal        -> Safe 1
      BERoundDecimal      -> Safe 1
      BEEqualNumeric      -> Safe 2
      BELessNumeric       -> Safe 2
      BELessEqNumeric     -> Safe 2
      BEGreaterNumeric    -> Safe 2
      BEGreaterEqNumeric  -> Safe 2
      BEAddNumeric        -> Safe 1
      BESubNumeric        -> Safe 1
      BEMulNumeric        -> Safe 1
      BEDivNumeric        -> Safe 1
      BEInt64ToNumeric    -> Safe 0
      BENumericToInt64    -> Safe 0
      BENumericFromText   -> Safe 1
      BEToTextNumeric     -> Safe 1
      BERoundNumeric      -> Safe 1
      BECastNumeric       -> Safe 0
      BEShiftNumeric      -> Safe 1
      BEAddInt64          -> Safe 1
      BESubInt64          -> Safe 1
      BEMulInt64          -> Safe 1
      BEDivInt64          -> Safe 1
      BEModInt64          -> Safe 1
      BEExpInt64          -> Safe 1
      BEInt64ToDecimal    -> Safe 1
      BEDecimalToInt64    -> Safe 0 -- crash if the decimal doesn't fit
      BEFoldl             -> Safe 2
      BEFoldr             -> Safe 2
      BETextMapEmpty      -> Safe 0
      BETextMapInsert     -> Safe 3
      BETextMapLookup     -> Safe 2
      BETextMapDelete     -> Safe 2
      BETextMapToList     -> Safe 1
      BETextMapSize       -> Safe 1
      BEGenMapEmpty       -> Safe 0
      BEGenMapInsert      -> Safe 2 -- crash if key invalid
      BEGenMapLookup      -> Safe 1 -- crash if key invalid
      BEGenMapDelete      -> Safe 1 -- crash if key invalid
      BEGenMapKeys        -> Safe 1
      BEGenMapValues      -> Safe 1
      BEGenMapSize        -> Safe 1
      BEEqualList         -> Safe 2 -- expects 3, 2-safe
      BEExplodeText       -> Safe 1
      BEImplodeText       -> Safe 1
      BESha256Text        -> Safe 1
      BEAppendText        -> Safe 2
      BETimestampToUnixMicroseconds -> Safe 1
      BEUnixMicrosecondsToTimestamp -> Safe 0 -- can fail if the int represents an out-of-bounds date
      BEDateToUnixDays -> Safe 1
      BEUnixDaysToDate -> Safe 0 -- can fail if the int represents an out-of-bounds date
      BETrace -> Unsafe -- we make it unsafe so that it never gets erased
      BEEqualContractId -> Safe 2
      BEPartyToQuotedText -> Safe 1
      BEPartyFromText -> Safe 1
      BEInt64FromText -> Safe 1
      BEDecimalFromText -> Safe 1
      BETextToCodePoints -> Safe 1
      BECoerceContractId -> Safe 1
      BETextToUpper -> Safe 1
      BETextToLower -> Safe 1
      BETextSlice -> Safe 3
      BETextSliceIndex -> Safe 2
      BETextContainsOnly -> Safe 2
      BETextReplicate -> Safe 2
      BETextSplitOn -> Safe 2
      BETextIntercalate -> Safe 2

  ERecConF _ fs -> minimum (Safe 0 : map snd fs)
  ERecProjF _ _ s -> s `min` Safe 0
  ERecUpdF _ _ s1 s2 -> s1 `min` s2 `min` Safe 0
  EVariantConF _ _ s -> s `min` Safe 0
  EEnumConF _ _ -> Safe 0
  EStructConF fs -> minimum (Safe 0 : map snd fs)
  EStructProjF _ s -> s `min` Safe 0
  EStructUpdF _ s1 s2 -> s1 `min` s2 `min` Safe 0
  ETmAppF s1 s2 ->
    case s2 of
      Unsafe -> Unsafe
      Safe _ -> decrSafety s1
  ETyAppF s _ -> s
  ETmLamF _ s -> incrSafety s
  ETyLamF _ s -> s
  ECaseF s1 as
    | Safe _ <- s1 -> Safe.minimumDef Unsafe (map snd as)
    | otherwise    -> Unsafe
  ELetF (BindingF _ s1) s2
    | Safe _ <- s1 -> s2
    | otherwise    -> Unsafe
  ENilF _ -> Safe 0
  EConsF _ s1 s2
    | Safe _ <- s1, Safe _ <- s2 -> Safe 0
    | otherwise                  -> Unsafe
  -- NOTE(MH): Updates and scenarios do probably not appear in positions related
  -- to the record boilerplate. If this changes, we need to revisit the next two
  -- cases.
  EUpdateF _ -> Unsafe
  EScenarioF _ -> Unsafe
  ELocationF _ s -> s
  ENoneF _ -> Safe 0
  ESomeF _ s
    | Safe _ <- s -> Safe 0
    | otherwise   -> Unsafe
  EToAnyF _ s
    | Safe _ <- s -> Safe 0
    | otherwise -> Unsafe
  EFromAnyF _ s
    | Safe _ <- s -> Safe 0
    | otherwise -> Unsafe
  ETypeRepF _ -> Safe 0
  EToAnyExceptionF _ s
    | Safe _ <- s -> Safe 0
    | otherwise -> Unsafe
  EFromAnyExceptionF _ s
    | Safe _ <- s -> Safe 0
    | otherwise -> Unsafe
  EThrowF _ _ _ -> Unsafe
  EExperimentalF _ _ -> Unsafe

isTypeClassDictionary :: DefValue -> Bool
isTypeClassDictionary DefValue{..}
    = T.isPrefixOf "$f" (unExprValName (fst dvalBinder)) -- generic dictionary
    || T.isPrefixOf "$d" (unExprValName (fst dvalBinder)) -- specialized dictionary

isTypeClassProjection :: DefValue -> Bool
isTypeClassProjection DefValue{..} = go dvalBody
  where
    go :: Expr -> Bool
    go (ETyLam _ e) = go e
    go (ETmLam _ e) = go e
    go (ETmApp e _) = go e
    go (EStructProj _ _) = True
    go _ = False

typeclassStep :: World -> ExprF TypeClassInfo -> TypeClassInfo
typeclassStep world = \case
    EValF x ->
        case lookupValue x world of
            Left _ -> TCNeither
            Right dv
                | isTypeClassProjection dv -> TCProjection (dvalBody dv)
                | isTypeClassDictionary dv -> TCDictionary (dvalBody dv)
                | otherwise -> TCNeither

    ETyAppF tci ty ->
        case tci of
            TCProjection (ETyLam (x,_) e) ->
                TCProjection (applySubstInExpr (typeSubst x ty) e)
            TCDictionary (ETyLam (x,_) e) ->
                TCDictionary (applySubstInExpr (typeSubst x ty) e)
            _ ->
                TCNeither

    _ -> TCNeither

infoStep :: World -> ExprF Info -> Info
infoStep world e = Info
    (freeVarsStep (fmap freeVars e))
    (safetyStep (fmap safety e))
    (typeclassStep world (fmap tcinfo e))

-- | Take the free variables and safety of a let-expression `let x = e1 in e2`
-- and compute over-approximations of the free variables and
-- under-approximations of the safe of `e1` and `e2`. The reasoning behind the
-- choice of `s1` and `s2` is as follows:
-- * If `fv(let x = e1 in e2) ⊆ V`, then `fv(e1) ⊆ V` and `fv(e2) ⊆ V ∪ {x}`.
-- * If `let x = e1 in e2` is k-safe, then `e1` is 0-safe and `e2` is k-safe.
infoUnstepELet :: ExprVarName -> Info -> (Info, Info)
infoUnstepELet x (Info fv sf _) = (s1, s2)
  where
    s1 = Info fv (sf `min` Safe 0) TCNeither
    s2 = Info (freeExprVar x <> fv) sf TCNeither

-- | Take the free variables and safety of a lambda-expression `λx. e1` and
-- compute an over-approximation of the free variables and an
-- under-approximation of the safety of `e1`. The reasoning behind the result
-- is as follows:
-- * If `fv(λx. e1) ⊆ V`, then `fv(e1) ⊆ V ∪ {x}`.
-- * If `λx. e1` is k-safe, then `e1` is (k-1)-safe.
infoUnstepETmapp :: ExprVarName -> Info -> Info
infoUnstepETmapp x (Info fv sf _) = Info (freeExprVar x <> fv) (decrSafety sf) TCNeither

-- | Try to get the actual field value from the body of
-- a typeclass projection function, after substitution of the
-- dictionary function inside.
getProjectedTypeclassField :: World -> Expr -> Maybe Expr
getProjectedTypeclassField world = \case
    EStructProj f e -> do
        EStructCon fs <- getTypeClassDictionary world e
        lookup f fs

    ETmApp e EUnit -> do
        ETmLam (x,_) e' <- getProjectedTypeclassField world e
        Just (applySubstInExpr (exprSubst x EUnit) e')

    _ ->
        Nothing

-- | Try to get typeclass dictionary from the body of
-- a typeclass dictionary function, after substitution.
-- This is made complicated by GHC's specializer, which
-- introduces a level of indirection. That's why we need
-- to inline dictionary functions and beta-reduce.
getTypeClassDictionary :: World -> Expr -> Maybe Expr
getTypeClassDictionary world = \case
    e@(EStructCon _) ->
        Just e

    EVal x
        | Right dv <- lookupValue x world
        , isTypeClassDictionary dv
        -> do
            Just (dvalBody dv)

    ETyApp e t -> do
        ETyLam (x,_) e' <- getTypeClassDictionary world e
        Just (applySubstInExpr (typeSubst x t) e')

    ETmApp e1 e2 -> do
        ETmLam (x,_) e1' <- getTypeClassDictionary world e1
        Just (applySubstInExpr (exprSubst x e2) e1')

    _ ->
        Nothing

calcPartyLiterals :: Expr -> HasNoPartyLiterals
calcPartyLiterals e = HasNoPartyLiterals (cata go e)
  where
    go = \case
        EBuiltinF (BEParty _) -> False
        f -> and f

-- | Attempt to lift a closed expression to the top level. Returns either
-- a variable expression that references the lifted expression, or
-- returns the original expression.
liftClosedExpr :: Expr -> Simplifier Expr
liftClosedExpr e = do
    cache <- gets sCache
    case Map.lookup e cache of
        Just name -> do
            EVal <$> selfQualify name

        Nothing -> do
            world <- gets sWorldExtended
            version <- gets sVersion
            case runGamma world version (typeOf' e) of
                Right ty -> do
                    name <- freshExprVarNameFor e
                    addDefValue DefValue
                        { dvalBinder = (name, ty)
                        , dvalBody = e
                        , dvalLocation = Nothing
                        , dvalNoPartyLiterals = calcPartyLiterals e
                        , dvalIsTest = IsTest False
                        }
                    EVal <$> selfQualify name

                -- This happens when the information in the World is incomplete, preventing
                -- full typechecking. That happens when compiling with --incremental=yes,
                -- or when simplifying mutually recursive functions.
                Left _ ->
                    pure e

-- | Remove top-level location information.
stripLoc :: Expr -> Expr
stripLoc = \case
  ELocation _ e -> stripLoc e
  e -> e

simplifyExpr :: Expr -> Simplifier Expr
simplifyExpr = fmap fst . cata go'
  where
    go' :: ExprF (Simplifier (Expr, Info)) -> Simplifier (Expr, Info)
    go' ms = do
        es <- sequence ms
        world <- gets sWorldExtended
        let v' = freeVarsStep (fmap (freeVars . snd) es)

        -- We decide here whether it's worth performing constant lifting
        -- for closed terms immediately under the current term. We want
        -- to avoid creating unnecessary bindings, so we only perform
        -- constant lifting when a closed term would become non-closed,
        -- thereby grouping all the closed subterms together into a single
        -- lift. If possible, we also want to lift constants from below
        -- lambdas and other binders (to make them memoizable),
        -- even if the resulting expression would remain closed,
        -- so we have the additional 'alwaysLiftUnder' check.
        if freeVarsNull v' && not (alwaysLiftUnder es)
          then pure (go world es)
          else do -- constant lifting
            es' <- forM es $ \case
              (e,i)
                | freeVarsNull (freeVars i)
                , isWorthLifting e
                -> do
                    e' <- liftClosedExpr e
                    pure (e',i)

                | otherwise
                -> pure (e,i)

            world' <- gets sWorldExtended
            pure (go world' es')

    go :: World -> ExprF (Expr, Info) -> (Expr, Info)
    go world = \case

      -- inline typeclass projections for known dictionaries
      ETmAppF (_, i1) (_, i2)
          | TCProjection (ETmLam (x,_) e1) <- tcinfo i1
          , TCDictionary e2 <- tcinfo i2
          , Just e' <- getProjectedTypeclassField world
              (applySubstInExpr (exprSubst' x e2 (freeVars i2)) e1)
                  -- (freeVars i2) is a safe over-approximation
                  -- of the free variables in e2, because e2 is
                  -- a repeated beta-reduction of a closed expression
                  -- (the dictionary function) applied to subterms of
                  -- the argument whose free variables are tracked in i2.
          -> cata (go world) e'

      -- inline typeclass projection for unknown dictionaries
      ETmAppF (_, i1) (e2, i2)
          | TCProjection (ETmLam (x,_) e1) <- tcinfo i1
          -> ( applySubstInExpr (exprSubst' x e2 (freeVars i2)) e1
             , infoStep world (ETmAppF i1 i2) )

      -- <...; f = e; ...>.f    ==>    e
      EStructProjF f (stripLoc -> EStructCon fes, s)
        -- NOTE(MH): We're deliberately overapproximating the potential of
        -- bottoms and the set of free variables below to avoid recomputing
        -- them.
        | Safe _ <- safety s, Just e <- f `lookup` fes -> (e, s)

      -- let x = e in x    ==>    e
      ELetF (BindingF (x1, _) e) (stripLoc -> EVar x2, _)
        | x1 == x2
        -> e

      -- let x = x in e    ==>    e
      ELetF (BindingF (x, _) (stripLoc -> EVar x', _)) e
        | x == x' -> e

      -- let x = <...; f = e; ...> in x.f    ==>    e
      ELetF (BindingF (x, _) (stripLoc -> EStructCon fes, s)) (stripLoc -> EStructProj f (stripLoc -> EVar x'), _)
        -- NOTE(MH): See NOTE above on @s@.
        | x == x', Safe _ <- safety s, Just e <- f `lookup` fes -> (e, s)

      -- let x = <f1 = e1; ...; fn = en> in T {f1 = x.f1; ...; fn = x.fn}
      -- ==>
      -- T {f1 = e1; ...; fn = en}
      ELetF (BindingF (x1, _) (stripLoc -> EStructCon fes1, s)) (stripLoc -> ERecCon t fes2, _)
        | Just bs <- Safe.zipWithExactMay matchField fes1 fes2
        , and bs ->
            (ERecCon t fes1, s)
        where
          matchField (f1, _) = \case
              (f2, stripLoc -> EStructProj f3 (stripLoc -> EVar x3)) ->
                  (f1 == f2) && (f1 == f3) && (x1 == x3)
              _ -> False

      -- let x = e1 in e2    ==>    e2, if e1 cannot be bottom and x is not free in e2
      ELetF (BindingF (x, _) e1) e2
        | Safe _ <- safety (snd e1)
        , not (isFreeExprVar x (freeVars (snd e2))) -> e2

      -- (let x = e1 in e2).f    ==>    let x = e1 in e2.f
      EStructProjF f (stripLoc -> ELet (Binding (x, t) e1) e2, s0) ->
          go world $ ELetF (BindingF (x, t) (e1, s1)) (go world $ EStructProjF f (e2, s2))
        where
          (s1, s2) = infoUnstepELet x s0

      -- (λx1 ... xn. e0) e1 ... en    ==>    let x1 = e2 in ... let xn = en in e0,
      -- if `xi` is not free in `ej` for any `i < j`
      --
      -- This rule is achieved by combining the rules for `(λx. e1) e2` and
      -- `(let x = e1 in e2) e3` repeatedly.

      -- (λx. e1) e2    ==>    let x = e2 in e1
      --
      -- NOTE(MH): This also works when `x` is free in `e2` since let-bindings
      -- are _not_ recursive.
      ETmAppF (stripLoc -> ETmLam (x, t) e1, s0) (e2, s2) ->
        go world $ ELetF (BindingF (x, t) (e2, s2)) (e1, s1)
        where
          s1 = infoUnstepETmapp x s0

      -- (let x = e1 in e2) e3    ==>    let x = e1 in e2 e3, if x is not free in e3
      ETmAppF (stripLoc -> ELet (Binding (x, t) e1) e2, s0) e3
        | not (isFreeExprVar x (freeVars (snd e3))) ->
          go world $ ELetF (BindingF (x, t) (e1, s1)) (go world $ ETmAppF (e2, s2) e3)
          where
            (s1, s2) = infoUnstepELet x s0

      -- e    ==>    e
      e -> (embed (fmap fst e), infoStep world (fmap snd e))

-- | If we have a closed term under a lambda, we want to lift it up to the top level,
-- even though the result of the lambda is also a closed term. We avoid breaking up
-- lambda terms, though.
alwaysLiftUnder :: ExprF (Expr, Info) -> Bool
alwaysLiftUnder = \case
    ETmLamF _ (ETmLam _ _, _) -> False
    ETmLamF _ _ -> True
    _ -> False

-- | Some terms are not worth lifting to the top level, because they don't
-- require any computation.
isWorthLifting :: Expr -> Bool
isWorthLifting = \case
    EVar _ -> False
    EVal _ -> False
    EBuiltin _ -> False
    EEnumCon _ _ -> False
    ENil _ -> False
    ENone _ -> False
    EUpdate _ -> False
    EScenario _ -> False
    ETypeRep _ -> False
    ETyApp e _ -> isWorthLifting e
    ETyLam _ e -> isWorthLifting e
    ELocation _ e -> isWorthLifting e
    _ -> True

data SimplifierState = SimplifierState
    { sWorld :: World
    , sVersion :: Version
    , sModule :: Module
    , sReserved :: Set.Set ExprValName
    , sCache :: Map.Map Expr ExprValName
    , sFreshNamePrefix :: T.Text -- Prefix for fresh variable names.
    }

sWorldExtended :: SimplifierState -> World
sWorldExtended SimplifierState{..} = extendWorldSelf sModule sWorld

type Simplifier t = State SimplifierState t

addDefValue :: DefValue -> Simplifier ()
addDefValue dval = modify $ \s@SimplifierState{..} -> s
    { sModule = sModule { moduleValues = NM.insert dval (moduleValues sModule) }
    , sReserved = Set.insert (fst (dvalBinder dval)) sReserved
    , sCache = Map.insert (dvalBody dval) (fst (dvalBinder dval)) sCache
    }

freshExprVarNameFor :: Expr -> Simplifier ExprValName
freshExprVarNameFor e = do
    name <- freshExprVarName
    modify $ \s -> s { sCache = Map.insert e name (sCache s) }
    pure name

setFreshNamePrefix :: T.Text -> Simplifier ()
setFreshNamePrefix x = modify (\s -> s { sFreshNamePrefix = x })

freshExprVarName :: Simplifier ExprValName
freshExprVarName = do
    reserved <- gets sReserved
    prefix <- gets sFreshNamePrefix
    let candidates = [ExprValName (prefix <> T.pack (show i)) | i <- [1 :: Int ..]]
        name = Safe.findJust (`Set.notMember` reserved) candidates
    modify (\s -> s { sReserved = Set.insert name reserved })
    pure name

selfQualify :: t -> Simplifier (Qualified t)
selfQualify qualObject = do
    qualModule <- gets (moduleName . sModule)
    let qualPackage = PRSelf
    pure Qualified {..}

exprRefs :: Expr -> Set.Set (Qualified ExprValName)
exprRefs = cata $ \case
    EValF x -> Set.singleton x
    e -> fold e

topoSortDefValues :: Module -> [DefValue]
topoSortDefValues m =
    let isLocal Qualified{..} = do
            PRSelf <- pure qualPackage
            guard (moduleName m == qualModule)
            Just qualObject
        dvalDeps = mapMaybe isLocal . Set.toList . exprRefs . dvalBody
        dvalName = fst . dvalBinder
        dvalNode dval = (dval, dvalName dval, dvalDeps dval)
        sccs = G.stronglyConnComp . map dvalNode . NM.toList $ moduleValues m
    in concatMap toList sccs

simplifyTemplate :: Template -> Simplifier Template
simplifyTemplate t = do
    setFreshNamePrefix ("$$sc_" <> T.intercalate "_" (unTypeConName (tplTypeCon t)) <> "_")
    templateExpr simplifyExpr t

simplifyModule :: World -> Version -> Module -> Module
simplifyModule world version m = runSimplifier world version m $ do
    forM_ (topoSortDefValues m) $ \ dval -> do
        setFreshNamePrefix ("$$sc_" <> unExprValName (fst (dvalBinder dval)) <> "_")
        body' <- simplifyExpr (dvalBody dval)
        addDefValue dval { dvalBody = body' }
    t' <- NM.traverse simplifyTemplate (moduleTemplates m)
    m' <- gets sModule
    pure m' { moduleTemplates = t' }

runSimplifier :: World -> Version -> Module -> Simplifier t -> t
runSimplifier sWorld sVersion m x =
    let sModule = m { moduleValues = NM.empty }
        sReserved = Set.fromList (NM.names (moduleValues m))
        sCache = Map.empty
        sFreshNamePrefix = "$$sc"
    in evalState x SimplifierState {..}
