-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Simplifier(
    freeVarsStep,
    simplifyModule,
    ) where

import Control.Arrow (second)
import Control.Lens hiding (para)
import Data.Functor.Foldable (cata, embed)
import qualified Data.Text as T
import qualified Safe
import qualified Safe.Exact as Safe

import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Subst
import DA.Daml.LF.Ast.Optics
import DA.Daml.LF.Ast.Recursive
import DA.Daml.LF.Ast.FreeVars

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

simplifyExpr :: World -> Expr -> Expr
simplifyExpr world = fst . cata go
  where

    go :: ExprF (Expr, Info) -> (Expr, Info)
    go = \case

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
          -> cata go e'

      -- <...; f = e; ...>.f    ==>    e
      EStructProjF f (EStructCon fes, s)
        -- NOTE(MH): We're deliberately overapproximating the potential of
        -- bottoms and the set of free variables below to avoid recomputing
        -- them.
        | Safe _ <- safety s, Just e <- f `lookup` fes -> (e, s)

      -- let x = e in x    ==>    e
      ELetF (BindingF (x, _) e) (EVar x', _)
        | x == x' -> e

      -- let x = x in e    ==>    e
      ELetF (BindingF (x, _) (EVar x', _)) e
        | x == x' -> e

      -- let x = <...; f = e; ...> in x.f    ==>    e
      ELetF (BindingF (x, _) (EStructCon fes, s)) (EStructProj f (EVar x'), _)
        -- NOTE(MH): See NOTE above on @s@.
        | x == x', Safe _ <- safety s, Just e <- f `lookup` fes -> (e, s)

      -- let x = <f1 = e1; ...; fn = en> in T {f1 = x.f1; ...; fn = x.fn}
      -- ==>
      -- T {f1 = e1; ...; fn = en}
      ELetF (BindingF (x1, _) (EStructCon fes1, s)) (ERecCon t fes2, _)
        | Just bs <- Safe.zipWithExactMay matchField fes1 fes2
        , and bs ->
            (ERecCon t fes1, s)
        where
          matchField (f1, _) (f2, e2)
            | f1 == f2, EStructProj f3 (EVar x3) <- e2, f1 == f3, x1 == x3 = True
            | otherwise = False

      -- let x = e1 in e2    ==>    e2, if e1 cannot be bottom and x is not free in e2
      ELetF (BindingF (x, _) e1) e2
        | Safe _ <- safety (snd e1)
        , not (isFreeExprVar x (freeVars (snd e2))) -> e2

      -- (let x = e1 in e2).f    ==>    let x = e1 in e2.f
      -- NOTE(MH): The reason for the choice of `s1` and `s2` is as follows:
      -- - If `fv(let x = e1 in e2) ⊆ V`, then `fv(e1) ⊆ V` and
      --   `fv(e2) ⊆ V ∪ {x}`.
      -- - If `let x = e1 in e2` is k-safe, then `e1` is 0-safe and `e2` is
      --   k-safe.
      EStructProjF f (ELet (Binding (x, t) e1) e2, Info fv sf _) ->
        go $ ELetF (BindingF (x, t) (e1, s1)) (go $ EStructProjF f (e2, s2))
        where
          s1 = Info fv (sf `min` Safe 0) TCNeither
          s2 = Info (freeExprVar x <> fv) sf TCNeither

      -- e    ==>    e
      e -> (embed (fmap fst e), infoStep world (fmap snd e))

-- Top-down inliner for simple functions.
inlineExpr :: World -> Expr -> Expr
inlineExpr _world = go
  where
    mkApp :: Expr -> Expr -> Expr
    mkApp (ELocation l e1) e2 = ELocation l (ETmApp e1 e2)
    mkApp e1 e2 = ETmApp e1 e2

    -- Special cases. We split the general cases into a separate function
    -- 'goExpr' so that GHC's pattern exhaustiveness checker works better.
    go :: Expr -> Expr
    go = \case
        EApps (EVal x) [TyArg _, TyArg _, TyArg _, TmArg f, TmArg g, TmArg h]
            | qualModule x == ModuleName ["GHC", "Base"]
            , qualObject x == ExprValName "."
            -> go (mkApp f (mkApp g h))

        EApps (EVal x) [TyArg _, TyArg _, TyArg t, TmArg f, TmArg g]
            | qualModule x == ModuleName ["GHC", "Base"]
            , qualObject x == ExprValName "."
            , let y = freshenExprVar (freeVarsInExpr f <> freeVarsInExpr g) (ExprVarName "y")
            -> go (ETmLam (y,t) (mkApp f (mkApp g (EVar y))))

        e -> goExpr e

    -- General cases.
    goExpr :: Expr -> Expr
    goExpr = \case
        e@(EVar _) -> e
        e@(EVal _) -> e
        e@(EBuiltin _) -> e
        ERecCon t fs -> ERecCon t (map (second go) fs)
        ERecProj t f e -> ERecProj t f (go e)
        ERecUpd t f e1 e2 -> ERecUpd t f (go e1) (go e2)
        EVariantCon t v e -> EVariantCon t v (go e)
        e@(EEnumCon _ _) -> e
        EStructCon fs -> EStructCon (map (second go) fs)
        EStructProj f e -> EStructProj f (go e)
        EStructUpd f e1 e2 -> EStructUpd f (go e1) (go e2)
        ETmApp e1 e2 -> ETmApp (go e1) (go e2)
        ETyApp e t -> ETyApp (go e) t
        ETmLam xt e -> ETmLam xt (go e)
        ETyLam xk e -> ETyLam xk (go e)
        ECase e as -> ECase (go e) (map goCaseAlternative as)
        ELet b e -> ELet (goBinding b) e
        e@(ENil _) -> e
        ECons t e1 e2 -> ECons t (go e1) (go e2)
        ESome t e -> ESome t (go e)
        e@(ENone _) -> e
        EToAny t e -> EToAny t (go e)
        EFromAny t e -> EFromAny t (go e)
        e@(ETypeRep _) -> e
        EUpdate u -> EUpdate (goUpdate u)
        EScenario s -> EScenario (goScenario s)
        ELocation l e -> ELocation l (go e)

    goBinding :: Binding -> Binding
    goBinding (Binding xt e) = Binding xt (go e)

    goCaseAlternative :: CaseAlternative -> CaseAlternative
    goCaseAlternative (CaseAlternative p e) = CaseAlternative p (go e)

    goUpdate :: Update -> Update
    goUpdate = \case
        UPure t e -> UPure t (go e)
        UBind b e -> UBind (goBinding b) (go e)
        UCreate t e -> UCreate t (go e)
        UExercise t c e1 e2M e3 -> UExercise t c (go e1) (go <$> e2M) (go e3)
        UFetch t e -> UFetch t (go e)
        e@UGetTime -> e
        UEmbedExpr t e -> UEmbedExpr t (go e)
        ULookupByKey r -> ULookupByKey (goRetrieveByKey r)
        UFetchByKey r -> UFetchByKey (goRetrieveByKey r)

    goRetrieveByKey :: RetrieveByKey -> RetrieveByKey
    goRetrieveByKey (RetrieveByKey t e) = RetrieveByKey t (go e)

    goScenario :: Scenario -> Scenario
    goScenario = \case
        SPure t e -> SPure t (go e)
        SBind b e -> SBind (goBinding b) (go e)
        SCommit t e1 e2 -> SCommit t (go e1) (go e2)
        SMustFailAt t e1 e2 -> SMustFailAt t (go e1) (go e2)
        SPass e -> SPass (go e)
        e@SGetTime -> e
        SGetParty e -> SGetParty (go e)
        SEmbedExpr t e -> SEmbedExpr t (go e)

simplifyModule :: World -> Module -> Module
simplifyModule world m0 =
    let m1 = over moduleExpr (inlineExpr (extendWorldSelf m0 world)) m0 -- top down
        m2 = over moduleExpr (simplifyExpr (extendWorldSelf m1 world)) m1 -- bottom up
    in m2
