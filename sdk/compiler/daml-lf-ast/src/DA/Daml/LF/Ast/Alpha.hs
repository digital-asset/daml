-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Alpha equivalence of types and expressions.
module DA.Daml.LF.Ast.Alpha
    ( alphaType
    , alphaExpr
    ) where

import qualified Data.Map.Strict as Map

import DA.Daml.LF.Ast.Base

-- | Auxiliary data structure to track bound variables.
data AlphaEnv = AlphaEnv
  { currentDepth :: !Int
    -- ^ Current binding depth.
  , boundTypeVarsLhs :: !(Map.Map TypeVarName Int)
    -- ^ Maps bound type variables from the left-hand-side to
    -- the depth of the binder which introduced them.
  , boundTypeVarsRhs :: !(Map.Map TypeVarName Int)
    -- ^ Maps bound type variables from the right-hand-side to
    -- the depth of the binder which introduced them.
  , boundExprVarsLhs :: !(Map.Map ExprVarName Int)
    -- ^ Maps bound expr variables from the left-hand-side  to
    -- the depth of the binder which introduced them.
  , boundExprVarsRhs :: !(Map.Map ExprVarName Int)
    -- ^ Maps bound expr variables from the right-hand-side to
    -- the depth of the binder which introduced them.
  }

onList :: (a -> a -> Bool) -> [a] -> [a] -> Bool
onList f xs ys = length xs == length ys
    && and (zipWith f xs ys)

onFieldList :: Eq a => (b -> b -> Bool) -> [(a,b)] -> [(a,b)] -> Bool
onFieldList f xs ys = map fst xs == map fst ys
    && and (zipWith f (map snd xs) (map snd ys))

bindTypeVar :: TypeVarName -> TypeVarName -> AlphaEnv -> AlphaEnv
bindTypeVar x1 x2 env@AlphaEnv{..} = env
    { currentDepth = currentDepth + 1
    , boundTypeVarsLhs = Map.insert x1 currentDepth boundTypeVarsLhs
    , boundTypeVarsRhs = Map.insert x2 currentDepth boundTypeVarsRhs }

bindExprVar :: ExprVarName -> ExprVarName -> AlphaEnv -> AlphaEnv
bindExprVar x1 x2 env@AlphaEnv{..} = env
    { currentDepth = currentDepth + 1
    , boundExprVarsLhs = Map.insert x1 currentDepth boundExprVarsLhs
    , boundExprVarsRhs = Map.insert x2 currentDepth boundExprVarsRhs }

alphaTypeVar :: AlphaEnv -> TypeVarName -> TypeVarName -> Bool
alphaTypeVar AlphaEnv{..} x1 x2 =
    case (Map.lookup x1 boundTypeVarsLhs, Map.lookup x2 boundTypeVarsRhs) of
        (Just l1, Just l2) -> l1 == l2
        (Nothing, Nothing) -> x1 == x2
        _ -> False

alphaExprVar :: AlphaEnv -> ExprVarName -> ExprVarName -> Bool
alphaExprVar AlphaEnv{..} x1 x2 =
    case (Map.lookup x1 boundExprVarsLhs, Map.lookup x2 boundExprVarsRhs) of
        (Just l1, Just l2) -> l1 == l2
        (Nothing, Nothing) -> x1 == x2
        _ -> False

-- | Strongly typed version of (==) for qualified type constructor names.
alphaTypeCon :: Qualified TypeConName -> Qualified TypeConName -> Bool
alphaTypeCon = (==)

-- | Strongly typed version of (==) for method names.
alphaMethod :: MethodName -> MethodName -> Bool
alphaMethod = (==)

alphaType' :: AlphaEnv -> Type -> Type -> Bool
alphaType' env = \case
    TVar x1 -> \case
        TVar x2 -> alphaTypeVar env x1 x2
        _ -> False
    TCon c1 -> \case
        TCon c2 -> alphaTypeCon c1 c2
        _ -> False
    TApp t1a t1b -> \case
        TApp t2a t2b -> alphaType' env t1a t2a && alphaType' env t1b t2b
        _ -> False
    TBuiltin b1 -> \case
        TBuiltin b2 -> b1 == b2
        _ -> False
    TForall (x1, k1) t1' -> \case
        TForall (x2, k2) t2' -> k1 == k2 &&
            let env' = bindTypeVar x1 x2 env
            in alphaType' env' t1' t2'
        _ -> False
    TStruct fs1 -> \case
        TStruct fs2 -> onFieldList (alphaType' env) fs1 fs2
        _ -> False
    TNat n1 -> \case
        TNat n2 -> n1 == n2
        _ -> False
    TSynApp s1 ts1 -> \case
        TSynApp s2 ts2 -> s1 == s2 && onList (alphaType' env) ts1 ts2
        _ -> False

alphaTypeConApp :: AlphaEnv -> TypeConApp -> TypeConApp -> Bool
alphaTypeConApp env (TypeConApp c1 ts1) (TypeConApp c2 ts2) =
    c1 == c2 && onList (alphaType' env) ts1 ts2

alphaExpr' :: AlphaEnv -> Expr -> Expr -> Bool
alphaExpr' env = \case
    EVar x1 -> \case
        EVar x2 -> alphaExprVar env x1 x2
        _ -> False
    EVal v1 -> \case
        EVal v2 -> v1 == v2
        _ -> False
    EBuiltin b1 -> \case
        EBuiltin b2 -> b1 == b2
        _ -> False
    ERecCon t1 fs1 -> \case
        ERecCon t2 fs2 -> alphaTypeConApp env t1 t2
            && onFieldList (alphaExpr' env) fs1 fs2
        _ -> False
    ERecProj t1 f1 e1 -> \case
        ERecProj t2 f2 e2 -> f1 == f2
            && alphaTypeConApp env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    ERecUpd t1 f1 e1a e1b -> \case
        ERecUpd t2 f2 e2a e2b -> f1 == f2
            && alphaTypeConApp env t1 t2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    EVariantCon t1 c1 e1 -> \case
        EVariantCon t2 c2 e2 -> c1 == c2
            && alphaTypeConApp env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    EEnumCon t1 c1 -> \case
        EEnumCon t2 c2 -> alphaTypeCon t1 t2 && c1 == c2
        _ -> False
    EStructCon fs1 -> \case
        EStructCon fs2 -> onFieldList (alphaExpr' env) fs1 fs2
        _ -> False
    EStructProj f1 e1 -> \case
        EStructProj f2 e2 -> f1 == f2
            && alphaExpr' env e1 e2
        _ -> False
    EStructUpd f1 e1a e1b -> \case
        EStructUpd f2 e2a e2b -> f1 == f2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    ETmApp e1a e1b -> \case
        ETmApp e2a e2b -> alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    ETyApp e1 t1 -> \case
        ETyApp e2 t2 -> alphaExpr' env e1 e2
            && alphaType' env t1 t2
        _ -> False
    ETmLam (x1,t1) e1 -> \case
        ETmLam (x2,t2) e2 -> alphaType' env t1 t2
            && alphaExpr' (bindExprVar x1 x2 env) e1 e2
        _ -> False
    ETyLam (x1,k1) e1 -> \case
        ETyLam (x2,k2) e2 -> k1 == k2
            && alphaExpr' (bindTypeVar x1 x2 env) e1 e2
        _ -> False
    ECase e1 ps1 -> \case
        ECase e2 ps2 -> alphaExpr' env e1 e2
            && onList (alphaCase env) ps1 ps2
        _ -> False
    ELet b1 e1 -> \case
        ELet b2 e2 ->
            alphaBinding env b1 b2 (\env' -> alphaExpr' env' e1 e2)
        _ -> False
    ENil t1 -> \case
        ENil t2 -> alphaType' env t1 t2
        _ -> False
    ECons t1 e1a e1b -> \case
        ECons t2 e2a e2b -> alphaType' env t1 t2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    ESome t1 e1 -> \case
        ESome t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    ENone t1 -> \case
        ENone t2 -> alphaType' env t1 t2
        _ -> False
    EToAny t1 e1 -> \case
        EToAny t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    EFromAny t1 e1 -> \case
        EFromAny t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    ETypeRep t1 -> \case
        ETypeRep t2 -> alphaType' env t1 t2
        _ -> False
    EToAnyException t1 e1 -> \case
        EToAnyException t2 e2
            -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    EFromAnyException t1 e1 -> \case
        EFromAnyException t2 e2
            -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    EThrow t1a t1b e1 -> \case
        EThrow t2a t2b e2
            -> alphaType' env t1a t2a
            && alphaType' env t1b t2b
            && alphaExpr' env e1 e2
        _ -> False
    EToInterface t1a t1b e1 -> \case
        EToInterface t2a t2b e2
            -> alphaTypeCon t1a t2a
            && alphaTypeCon t1b t2b
            && alphaExpr' env e1 e2
        _ -> False
    EFromInterface t1a t1b e1 -> \case
        EFromInterface t2a t2b e2
            -> alphaTypeCon t1a t2a
            && alphaTypeCon t1b t2b
            && alphaExpr' env e1 e2
        _ -> False
    EUnsafeFromInterface t1a t1b e1a e1b -> \case
        EUnsafeFromInterface t2a t2b e2a e2b
            -> alphaTypeCon t1a t2a
            && alphaTypeCon t1b t2b
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    ECallInterface t1 m1 e1 -> \case
        ECallInterface t2 m2 e2
            -> alphaTypeCon t1 t2
            && alphaMethod m1 m2
            && alphaExpr' env e1 e2
        _ -> False
    EToRequiredInterface t1a t1b e1 -> \case
        EToRequiredInterface t2a t2b e2
            -> alphaTypeCon t1a t2a
            && alphaTypeCon t1b t2b
            && alphaExpr' env e1 e2
        _ -> False
    EFromRequiredInterface t1a t1b e1 -> \case
        EFromRequiredInterface t2a t2b e2
            -> alphaTypeCon t1a t2a
            && alphaTypeCon t1b t2b
            && alphaExpr' env e1 e2
        _ -> False
    EUnsafeFromRequiredInterface t1a t1b e1a e1b -> \case
        EUnsafeFromRequiredInterface t2a t2b e2a e2b
            -> alphaTypeCon t1a t2a
            && alphaTypeCon t1b t2b
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    EInterfaceTemplateTypeRep ty1 expr1 -> \case
        EInterfaceTemplateTypeRep ty2 expr2
            -> alphaTypeCon ty1 ty2
            && alphaExpr' env expr1 expr2
        _ -> False
    ESignatoryInterface ty1 expr1 -> \case
        ESignatoryInterface ty2 expr2
            -> alphaTypeCon ty1 ty2
            && alphaExpr' env expr1 expr2
        _ -> False
    EObserverInterface ty1 expr1 -> \case
        EObserverInterface ty2 expr2
            -> alphaTypeCon ty1 ty2
            && alphaExpr' env expr1 expr2
        _ -> False
    EUpdate u1 -> \case
        EUpdate u2 -> alphaUpdate env u1 u2
        _ -> False
    EScenario s1 -> \case
        EScenario s2 -> alphaScenario env s1 s2
        _ -> False
    ELocation _ e1 -> \case
        ELocation _ e2 -> alphaExpr' env e1 e2
        _ -> False
    EViewInterface iface1 expr1 -> \case
        EViewInterface iface2 expr2
            -> alphaTypeCon iface1 iface2
            && alphaExpr' env expr1 expr2
        _ -> False
    EChoiceController t1 ch1 e1a e1b -> \case
        EChoiceController t2 ch2 e2a e2b
            -> alphaTypeCon t1 t2
            && ch1 == ch2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    EChoiceObserver t1 ch1 e1a e1b -> \case
        EChoiceObserver t2 ch2 e2a e2b
            -> alphaTypeCon t1 t2
            && ch1 == ch2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    EExperimental n1 t1 -> \case
        EExperimental n2 t2 -> n1 == n2 && alphaType t1 t2
        _ -> False

alphaBinding :: AlphaEnv -> Binding -> Binding -> (AlphaEnv -> Bool) -> Bool
alphaBinding env (Binding (x1,t1) e1) (Binding (x2,t2) e2) k =
    alphaType' env t1 t2 && alphaExpr' env e1 e2 && k (bindExprVar x1 x2 env)

alphaCase :: AlphaEnv -> CaseAlternative -> CaseAlternative -> Bool
alphaCase env (CaseAlternative p1 e1) (CaseAlternative p2 e2) =
    alphaPattern env p1 p2 (\env' -> alphaExpr' env' e1 e2)

alphaPattern :: AlphaEnv -> CasePattern -> CasePattern -> (AlphaEnv -> Bool) -> Bool
alphaPattern env p1 p2 k = case p1 of
    CPVariant t1 c1 x1 -> case p2 of
        CPVariant t2 c2 x2 -> alphaTypeCon t1 t2 && c1 == c2 && k (bindExprVar x1 x2 env)
        _ -> False
    CPEnum t1 c1 -> case p2 of
        CPEnum t2 c2 -> alphaTypeCon t1 t2 && c1 == c2 && k env
        _ -> False
    CPUnit -> case p2 of
        CPUnit -> k env
        _ -> False
    CPBool b1 -> case p2 of
        CPBool b2 -> b1 == b2 && k env
        _ -> False
    CPNil -> case p2 of
        CPNil -> k env
        _ -> False
    CPCons x1a x1b -> case p2 of
        CPCons x2a x2b -> k (bindExprVar x1a x2a (bindExprVar x1b x2b env))
        _ -> False
    CPNone -> case p2 of
        CPNone -> k env
        _ -> False
    CPSome x1 -> case p2 of
        CPSome x2 -> k (bindExprVar x1 x2 env)
        _ -> False
    CPDefault -> case p2 of
        CPDefault -> k env
        _ -> False

alphaUpdate :: AlphaEnv -> Update -> Update -> Bool
alphaUpdate env = \case
    UPure t1 e1 -> \case
        UPure t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    UBind b1 e1 -> \case
        UBind b2 e2 ->
            alphaBinding env b1 b2 (\env' -> alphaExpr' env' e1 e2)
        _ -> False
    UCreate t1 e1 -> \case
        UCreate t2 e2 -> alphaTypeCon t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    UCreateInterface t1 e1 -> \case
        UCreateInterface t2 e2 -> alphaTypeCon t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    UExercise t1 c1 e1a e1b -> \case
        UExercise t2 c2 e2a e2b -> alphaTypeCon t1 t2
            && c1 == c2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    USoftExercise t1 c1 e1a e1b -> \case
        USoftExercise t2 c2 e2a e2b -> alphaTypeCon t1 t2
            && c1 == c2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    UDynamicExercise t1 c1 e1a e1b -> \case
        UDynamicExercise t2 c2 e2a e2b -> alphaTypeCon t1 t2
            && c1 == c2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    UExerciseInterface i1 c1 e1a e1b e1c -> \case
        UExerciseInterface i2 c2 e2a e2b e2c ->
            let eqMaybe1 f (Just a) (Just b) = f a b
                eqMaybe1 _ Nothing Nothing = True
                eqMaybe1 _ _ _ = False
            in
            alphaTypeCon i1 i2
            && c1 == c2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
            && eqMaybe1 (alphaExpr' env) e1c e2c
        _ -> False
    UExerciseByKey t1 c1 e1a e1b -> \case
        UExerciseByKey t2 c2 e2a e2b -> alphaTypeCon t1 t2
            && c1 == c2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    UFetch t1 e1 -> \case
        UFetch t2 e2 -> alphaTypeCon t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    USoftFetch t1 e1 -> \case
        USoftFetch t2 e2 -> alphaTypeCon t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    UFetchInterface i1 e1 -> \case
        UFetchInterface i2 e2 -> alphaTypeCon i1 i2
            && alphaExpr' env e1 e2
        _ -> False
    UGetTime -> \case
        UGetTime -> True
        _ -> False
    UEmbedExpr t1 e1 -> \case
        UEmbedExpr t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    ULookupByKey r1 -> \case
        ULookupByKey r2 -> alphaRetrieveByKey env r1 r2
        _ -> False
    UFetchByKey r1 -> \case
        UFetchByKey r2 -> alphaRetrieveByKey env r1 r2
        _ -> False
    UTryCatch t1 e1a x1 e1b -> \case
        UTryCatch t2 e2a x2 e2b -> alphaType' env t1 t2
            && alphaExpr' env e1a e2a
            && alphaExpr' (bindExprVar x1 x2 env) e1b e2b
        _ -> False

alphaRetrieveByKey :: AlphaEnv -> RetrieveByKey -> RetrieveByKey -> Bool
alphaRetrieveByKey env (RetrieveByKey t1 e1) (RetrieveByKey t2 e2) =
    alphaTypeCon t1 t2 && alphaExpr' env e1 e2

alphaScenario :: AlphaEnv -> Scenario -> Scenario -> Bool
alphaScenario env = \case
    SPure t1 e1 -> \case
        SPure t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False
    SBind b1 e1 -> \case
        SBind b2 e2 ->
            alphaBinding env b1 b2 (\env' -> alphaExpr' env' e1 e2)
        _ -> False
    SCommit t1 e1a e1b -> \case
        SCommit t2 e2a e2b -> alphaType' env t1 t2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    SMustFailAt t1 e1a e1b -> \case
        SMustFailAt t2 e2a e2b -> alphaType' env t1 t2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> False
    SPass e1 -> \case
        SPass e2 -> alphaExpr' env e1 e2
        _ -> False
    SGetTime -> \case
        SGetTime -> True
        _ -> False
    SGetParty e1 -> \case
        SGetParty e2 -> alphaExpr' env e1 e2
        _ -> False
    SEmbedExpr t1 e1 -> \case
        SEmbedExpr t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> False

initialAlphaEnv :: AlphaEnv
initialAlphaEnv = AlphaEnv
    { currentDepth = 0
    , boundTypeVarsLhs = Map.empty
    , boundTypeVarsRhs = Map.empty
    , boundExprVarsLhs = Map.empty
    , boundExprVarsRhs = Map.empty
    }

alphaType :: Type -> Type -> Bool
alphaType = alphaType' initialAlphaEnv

alphaExpr :: Expr -> Expr -> Bool
alphaExpr = alphaExpr' initialAlphaEnv
