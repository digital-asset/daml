-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Alpha equivalence of types and expressions.
module DA.Daml.LF.Ast.Alpha
    ( alphaType
    , alphaExpr
    , alphaType'
    , alphaExpr'
    , initialAlphaEnv
    , alphaTypeConDefault
    , bindTypeVar
    , AlphaEnv(..)
    , Mismatch(..)
    , SomeName(..)
    , IsSomeName(..)
    , toMismatch
    , nameMismatch
    , alphaEq
    , alphaEq'
    , andMismatches
    ) where

import qualified Data.Map.Strict as Map

import DA.Daml.LF.Ast.Base

import Prelude hiding ((&&))
import qualified Prelude as P

-- Datatypes for tracking origins of mismatches between types and between
-- expressions
data Mismatch reason
  = NameMismatch SomeName SomeName (Maybe reason)
  | BindingMismatch SomeName SomeName
  | StructuralMismatch
  deriving (Show, Eq, Ord)

type Mismatches reason = [Mismatch reason]

data SomeName
  = SNTypeVarName TypeVarName
  | SNExprVarName ExprVarName
  | SNTypeConName TypeConName
  | SNExprValName ExprValName
  | SNFieldName FieldName
  | SNChoiceName ChoiceName
  | SNTypeSynName TypeSynName
  | SNVariantConName VariantConName
  | SNMethodName MethodName
  | SNQualified (Qualified SomeName)
  deriving (Show, Eq, Ord)

class (Eq a, Show a) => IsSomeName a where
  toSomeName :: a -> SomeName

instance IsSomeName TypeVarName where toSomeName = SNTypeVarName
instance IsSomeName ExprVarName where toSomeName = SNExprVarName
instance IsSomeName TypeConName where toSomeName = SNTypeConName
instance IsSomeName ExprValName where toSomeName = SNExprValName
instance IsSomeName FieldName where toSomeName = SNFieldName
instance IsSomeName ChoiceName where toSomeName = SNChoiceName
instance IsSomeName TypeSynName where toSomeName = SNTypeSynName
instance IsSomeName VariantConName where toSomeName = SNVariantConName
instance IsSomeName MethodName where toSomeName = SNMethodName
instance IsSomeName a => IsSomeName (Qualified a) where
  toSomeName = SNQualified . fmap toSomeName

bindingMismatch :: IsSomeName a => a -> a -> Mismatch reason
bindingMismatch l r = BindingMismatch (toSomeName l) (toSomeName r)

structuralMismatch :: Mismatches reason
structuralMismatch = [StructuralMismatch]

andMismatches, (&&) :: Mismatches reason -> Mismatches reason -> Mismatches reason
andMismatches = (P.<>)
(&&) = andMismatches

infixr 3 &&

alphaEq :: Eq a => Mismatch reason -> a -> a -> Mismatches reason
alphaEq msg x y = if x == y then noMismatch else [msg]

alphaEq' :: (IsSomeName a) => a -> a -> Mismatches reason
alphaEq' x y = alphaEq (nameMismatch x y Nothing) x y

nameMismatch :: IsSomeName a => a -> a -> Maybe reason -> Mismatch reason
nameMismatch x y reason = NameMismatch (toSomeName x) (toSomeName y) reason

eqStructural :: (Eq a) => a -> a -> Mismatches reason
eqStructural = alphaEq StructuralMismatch

allMismatches :: [Mismatches reason] -> Mismatches reason
allMismatches = concat

noMismatch :: Mismatches reason
noMismatch = []

toMismatch :: P.Bool -> Mismatch reason -> Mismatches reason
toMismatch b m = if b then noMismatch else [m]

-- | Auxiliary data structure to track bound variables.
data AlphaEnv reason = AlphaEnv
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
  , alphaTypeCon :: !(Qualified TypeConName -> Qualified TypeConName -> Mismatches reason)
    -- ^ Defines how names in typecons should be compared
    -- Unlike above fields, this should not mutate over the course of the alpha
    -- equivalence check
  , alphaExprVal :: !(Qualified ExprValName -> Qualified ExprValName -> Mismatches reason)
    -- ^ Defines how names in expressions should be compared
    -- Unlike above fields, this should not mutate over the course of the alpha
    -- equivalence check
  }

onList :: (a -> a -> Mismatches reason) -> [a] -> [a] -> Mismatches reason
onList f xs ys = alphaEq StructuralMismatch (length xs) (length ys)
    && allMismatches (zipWith f xs ys)

onFieldList :: (IsSomeName a) => (b -> b -> Mismatches reason) -> [(a,b)] -> [(a,b)] -> Mismatches reason
onFieldList f xs ys = allMismatches (zipWith alphaEq' (map fst xs) (map fst ys))
    && allMismatches (zipWith f (map snd xs) (map snd ys))

bindTypeVar :: TypeVarName -> TypeVarName -> AlphaEnv reason -> AlphaEnv reason
bindTypeVar x1 x2 env@AlphaEnv{..} = env
    { currentDepth = currentDepth + 1
    , boundTypeVarsLhs = Map.insert x1 currentDepth boundTypeVarsLhs
    , boundTypeVarsRhs = Map.insert x2 currentDepth boundTypeVarsRhs }

bindExprVar :: ExprVarName -> ExprVarName -> AlphaEnv reason -> AlphaEnv reason
bindExprVar x1 x2 env@AlphaEnv{..} = env
    { currentDepth = currentDepth + 1
    , boundExprVarsLhs = Map.insert x1 currentDepth boundExprVarsLhs
    , boundExprVarsRhs = Map.insert x2 currentDepth boundExprVarsRhs }

alphaTypeVar :: AlphaEnv reason -> TypeVarName -> TypeVarName -> Mismatches reason
alphaTypeVar AlphaEnv{..} x1 x2 =
    case (Map.lookup x1 boundTypeVarsLhs, Map.lookup x2 boundTypeVarsRhs) of
        (Just l1, Just l2) -> toMismatch (l1 == l2) (bindingMismatch x1 x2)
        (Nothing, Nothing) -> alphaEq' x1 x2
        _ -> [bindingMismatch x1 x2]

alphaExprVar :: AlphaEnv reason -> ExprVarName -> ExprVarName -> Mismatches reason
alphaExprVar AlphaEnv{..} x1 x2 =
    case (Map.lookup x1 boundExprVarsLhs, Map.lookup x2 boundExprVarsRhs) of
        (Just l1, Just l2) -> toMismatch (l1 == l2) (bindingMismatch x1 x2)
        (Nothing, Nothing) -> alphaEq' x1 x2
        _ -> [bindingMismatch x1 x2]

-- | Strongly typed version of alphaEq' for qualified type constructor names.
alphaTypeConDefault :: Qualified TypeConName -> Qualified TypeConName -> Mismatches reason
alphaTypeConDefault = alphaEq'

-- | Strongly typed version of alphaEq' for qualified expression value names.
alphaExprValDefault :: Qualified ExprValName -> Qualified ExprValName -> Mismatches reason
alphaExprValDefault = alphaEq'

-- | Strongly typed version of alphaEq' for method names.
alphaMethod :: MethodName -> MethodName -> Mismatches reason
alphaMethod = alphaEq'

alphaType' :: AlphaEnv reason -> Type -> Type -> Mismatches reason
alphaType' env = \case
    TVar x1 -> \case
        TVar x2 -> alphaTypeVar env x1 x2
        _ -> structuralMismatch
    TCon c1 -> \case
        TCon c2 -> alphaTypeCon env c1 c2
        _ -> structuralMismatch
    TApp t1a t1b -> \case
        TApp t2a t2b -> alphaType' env t1a t2a && alphaType' env t1b t2b
        _ -> structuralMismatch
    TBuiltin b1 -> \case
        TBuiltin b2 -> b1 `eqStructural` b2
        _ -> structuralMismatch
    TForall (x1, k1) t1' -> \case
        TForall (x2, k2) t2' -> k1 `eqStructural` k2 &&
            let env' = bindTypeVar x1 x2 env
            in alphaType' env' t1' t2'
        _ -> structuralMismatch
    TStruct fs1 -> \case
        TStruct fs2 -> onFieldList (alphaType' env) fs1 fs2
        _ -> structuralMismatch
    TNat n1 -> \case
        TNat n2 -> n1 `eqStructural` n2
        _ -> structuralMismatch
    TSynApp s1 ts1 -> \case
        TSynApp s2 ts2 -> alphaEq' s1 s2 && onList (alphaType' env) ts1 ts2
        _ -> structuralMismatch

alphaTypeConApp :: AlphaEnv reason -> TypeConApp -> TypeConApp -> Mismatches reason
alphaTypeConApp env (TypeConApp c1 ts1) (TypeConApp c2 ts2) =
    alphaTypeCon env c1 c2 && onList (alphaType' env) ts1 ts2

alphaExpr' :: AlphaEnv reason -> Expr -> Expr -> Mismatches reason
alphaExpr' env = \case
    EVar x1 -> \case
        EVar x2 -> alphaExprVar env x1 x2
        _ -> structuralMismatch
    EVal v1 -> \case
        EVal v2 -> alphaExprVal env v1 v2
        _ -> structuralMismatch
    EBuiltinFun b1 -> \case
        EBuiltinFun b2 -> b1 `eqStructural` b2
        _ -> structuralMismatch
    ERecCon t1 fs1 -> \case
        ERecCon t2 fs2 -> alphaTypeConApp env t1 t2
            && onFieldList (alphaExpr' env) fs1 fs2
        _ -> structuralMismatch
    ERecProj t1 f1 e1 -> \case
        ERecProj t2 f2 e2 -> alphaEq' f1 f2
            && alphaTypeConApp env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    ERecUpd t1 f1 e1a e1b -> \case
        ERecUpd t2 f2 e2a e2b -> alphaEq' f1 f2
            && alphaTypeConApp env t1 t2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    EVariantCon t1 c1 e1 -> \case
        EVariantCon t2 c2 e2 -> alphaEq' c1 c2
            && alphaTypeConApp env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EEnumCon t1 c1 -> \case
        EEnumCon t2 c2 -> alphaTypeCon env t1 t2 && alphaEq' c1 c2
        _ -> structuralMismatch
    EStructCon fs1 -> \case
        EStructCon fs2 -> onFieldList (alphaExpr' env) fs1 fs2
        _ -> structuralMismatch
    EStructProj f1 e1 -> \case
        EStructProj f2 e2 -> alphaEq' f1 f2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EStructUpd f1 e1a e1b -> \case
        EStructUpd f2 e2a e2b -> alphaEq' f1 f2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    ETmApp e1a e1b -> \case
        ETmApp e2a e2b -> alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    ETyApp e1 t1 -> \case
        ETyApp e2 t2 -> alphaExpr' env e1 e2
            && alphaType' env t1 t2
        _ -> structuralMismatch
    ETmLam (x1,t1) e1 -> \case
        ETmLam (x2,t2) e2 -> alphaType' env t1 t2
            && alphaExpr' (bindExprVar x1 x2 env) e1 e2
        _ -> structuralMismatch
    ETyLam (x1,k1) e1 -> \case
        ETyLam (x2,k2) e2 -> k1 `eqStructural` k2
            && alphaExpr' (bindTypeVar x1 x2 env) e1 e2
        _ -> structuralMismatch
    ECase e1 ps1 -> \case
        ECase e2 ps2 -> alphaExpr' env e1 e2
            && onList (alphaCase env) ps1 ps2
        _ -> structuralMismatch
    ELet b1 e1 -> \case
        ELet b2 e2 ->
            alphaBinding env b1 b2 (\env' -> alphaExpr' env' e1 e2)
        _ -> structuralMismatch
    ENil t1 -> \case
        ENil t2 -> alphaType' env t1 t2
        _ -> structuralMismatch
    ECons t1 e1a e1b -> \case
        ECons t2 e2a e2b -> alphaType' env t1 t2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    ESome t1 e1 -> \case
        ESome t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    ENone t1 -> \case
        ENone t2 -> alphaType' env t1 t2
        _ -> structuralMismatch
    EToAny t1 e1 -> \case
        EToAny t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EFromAny t1 e1 -> \case
        EFromAny t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    ETypeRep t1 -> \case
        ETypeRep t2 -> alphaType' env t1 t2
        _ -> structuralMismatch
    EToAnyException t1 e1 -> \case
        EToAnyException t2 e2
            -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EFromAnyException t1 e1 -> \case
        EFromAnyException t2 e2
            -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EThrow t1a t1b e1 -> \case
        EThrow t2a t2b e2
            -> alphaType' env t1a t2a
            && alphaType' env t1b t2b
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EToInterface t1a t1b e1 -> \case
        EToInterface t2a t2b e2
            -> alphaTypeCon env t1a t2a
            && alphaTypeCon env t1b t2b
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EFromInterface t1a t1b e1 -> \case
        EFromInterface t2a t2b e2
            -> alphaTypeCon env t1a t2a
            && alphaTypeCon env t1b t2b
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EUnsafeFromInterface t1a t1b e1a e1b -> \case
        EUnsafeFromInterface t2a t2b e2a e2b
            -> alphaTypeCon env t1a t2a
            && alphaTypeCon env t1b t2b
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    ECallInterface t1 m1 e1 -> \case
        ECallInterface t2 m2 e2
            -> alphaTypeCon env t1 t2
            && alphaMethod m1 m2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EToRequiredInterface t1a t1b e1 -> \case
        EToRequiredInterface t2a t2b e2
            -> alphaTypeCon env t1a t2a
            && alphaTypeCon env t1b t2b
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EFromRequiredInterface t1a t1b e1 -> \case
        EFromRequiredInterface t2a t2b e2
            -> alphaTypeCon env t1a t2a
            && alphaTypeCon env t1b t2b
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    EUnsafeFromRequiredInterface t1a t1b e1a e1b -> \case
        EUnsafeFromRequiredInterface t2a t2b e2a e2b
            -> alphaTypeCon env t1a t2a
            && alphaTypeCon env t1b t2b
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    EInterfaceTemplateTypeRep ty1 expr1 -> \case
        EInterfaceTemplateTypeRep ty2 expr2
            -> alphaTypeCon env ty1 ty2
            && alphaExpr' env expr1 expr2
        _ -> structuralMismatch
    ESignatoryInterface ty1 expr1 -> \case
        ESignatoryInterface ty2 expr2
            -> alphaTypeCon env ty1 ty2
            && alphaExpr' env expr1 expr2
        _ -> structuralMismatch
    EObserverInterface ty1 expr1 -> \case
        EObserverInterface ty2 expr2
            -> alphaTypeCon env ty1 ty2
            && alphaExpr' env expr1 expr2
        _ -> structuralMismatch
    EUpdate u1 -> \case
        EUpdate u2 -> alphaUpdate env u1 u2
        _ -> structuralMismatch
    ELocation _ e1 -> \case
        ELocation _ e2 -> alphaExpr' env e1 e2
        _ -> structuralMismatch
    EViewInterface iface1 expr1 -> \case
        EViewInterface iface2 expr2
            -> alphaTypeCon env iface1 iface2
            && alphaExpr' env expr1 expr2
        _ -> structuralMismatch
    EChoiceController t1 ch1 e1a e1b -> \case
        EChoiceController t2 ch2 e2a e2b
            -> alphaTypeCon env t1 t2
            && alphaEq' ch1 ch2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    EChoiceObserver t1 ch1 e1a e1b -> \case
        EChoiceObserver t2 ch2 e2a e2b
            -> alphaTypeCon env t1 t2
            && alphaEq' ch1 ch2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    EExperimental n1 t1 -> \case
        EExperimental n2 t2 -> n1 `eqStructural` n2 && alphaType' env t1 t2
        _ -> structuralMismatch

alphaBinding :: AlphaEnv reason -> Binding -> Binding -> (AlphaEnv reason -> Mismatches reason) -> Mismatches reason
alphaBinding env (Binding (x1,t1) e1) (Binding (x2,t2) e2) k =
    alphaType' env t1 t2 && alphaExpr' env e1 e2 && k (bindExprVar x1 x2 env)

alphaCase :: AlphaEnv reason -> CaseAlternative -> CaseAlternative -> Mismatches reason
alphaCase env (CaseAlternative p1 e1) (CaseAlternative p2 e2) =
    alphaPattern env p1 p2 (\env' -> alphaExpr' env' e1 e2)

alphaPattern :: AlphaEnv reason -> CasePattern -> CasePattern -> (AlphaEnv reason -> Mismatches reason) -> Mismatches reason
alphaPattern env p1 p2 k = case p1 of
    CPVariant t1 c1 x1 -> case p2 of
        CPVariant t2 c2 x2 -> alphaTypeCon env t1 t2 && alphaEq' c1 c2 && k (bindExprVar x1 x2 env)
        _ -> structuralMismatch
    CPEnum t1 c1 -> case p2 of
        CPEnum t2 c2 -> alphaTypeCon env t1 t2 && alphaEq' c1 c2 && k env
        _ -> structuralMismatch
    CPUnit -> case p2 of
        CPUnit -> k env
        _ -> structuralMismatch
    CPBool b1 -> case p2 of
        CPBool b2 -> b1 `eqStructural` b2 && k env
        _ -> structuralMismatch
    CPNil -> case p2 of
        CPNil -> k env
        _ -> structuralMismatch
    CPCons x1a x1b -> case p2 of
        CPCons x2a x2b -> k (bindExprVar x1a x2a (bindExprVar x1b x2b env))
        _ -> structuralMismatch
    CPNone -> case p2 of
        CPNone -> k env
        _ -> structuralMismatch
    CPSome x1 -> case p2 of
        CPSome x2 -> k (bindExprVar x1 x2 env)
        _ -> structuralMismatch
    CPDefault -> case p2 of
        CPDefault -> k env
        _ -> structuralMismatch

alphaUpdate :: AlphaEnv reason -> Update -> Update -> Mismatches reason
alphaUpdate env = \case
    UPure t1 e1 -> \case
        UPure t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    UBind b1 e1 -> \case
        UBind b2 e2 ->
            alphaBinding env b1 b2 (\env' -> alphaExpr' env' e1 e2)
        _ -> structuralMismatch
    UCreate t1 e1 -> \case
        UCreate t2 e2 -> alphaTypeCon env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    UCreateInterface t1 e1 -> \case
        UCreateInterface t2 e2 -> alphaTypeCon env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    UExercise t1 c1 e1a e1b -> \case
        UExercise t2 c2 e2a e2b -> alphaTypeCon env t1 t2
            && alphaEq' c1 c2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    UExerciseInterface i1 c1 e1a e1b e1c -> \case
        UExerciseInterface i2 c2 e2a e2b e2c ->
            let eqMaybe1 f (Just a) (Just b) = f a b
                eqMaybe1 _ Nothing Nothing = noMismatch
                eqMaybe1 _ _ _ = structuralMismatch
            in
            alphaTypeCon env i1 i2
            && alphaEq' c1 c2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
            && eqMaybe1 (alphaExpr' env) e1c e2c
        _ -> structuralMismatch
    UExerciseByKey t1 c1 e1a e1b -> \case
        UExerciseByKey t2 c2 e2a e2b -> alphaTypeCon env t1 t2
            && alphaEq' c1 c2
            && alphaExpr' env e1a e2a
            && alphaExpr' env e1b e2b
        _ -> structuralMismatch
    UFetch t1 e1 -> \case
        UFetch t2 e2 -> alphaTypeCon env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    UFetchInterface i1 e1 -> \case
        UFetchInterface i2 e2 -> alphaTypeCon env i1 i2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    UGetTime -> \case
        UGetTime -> noMismatch
        _ -> structuralMismatch
    ULedgerTimeLT e1 -> \case
        ULedgerTimeLT e2 -> alphaExpr' env e1 e2
        _ -> structuralMismatch
    UEmbedExpr t1 e1 -> \case
        UEmbedExpr t2 e2 -> alphaType' env t1 t2
            && alphaExpr' env e1 e2
        _ -> structuralMismatch
    ULookupByKey r1 -> \case
        ULookupByKey r2 -> alphaTypeCon env r1 r2
        _ -> structuralMismatch
    UQueryNByKey r1 -> \case
        UQueryNByKey r2 -> alphaTypeCon env r1 r2
        _ -> structuralMismatch
    UFetchByKey r1 -> \case
        UFetchByKey r2 -> alphaTypeCon env r1 r2
        _ -> structuralMismatch
    UTryCatch t1 e1a x1 e1b -> \case
        UTryCatch t2 e2a x2 e2b -> alphaType' env t1 t2
            && alphaExpr' env e1a e2a
            && alphaExpr' (bindExprVar x1 x2 env) e1b e2b
        _ -> structuralMismatch

initialAlphaEnv :: AlphaEnv reason
initialAlphaEnv = AlphaEnv
    { currentDepth = 0
    , boundTypeVarsLhs = Map.empty
    , boundTypeVarsRhs = Map.empty
    , boundExprVarsLhs = Map.empty
    , boundExprVarsRhs = Map.empty
    , alphaTypeCon = alphaTypeConDefault
    , alphaExprVal = alphaExprValDefault
    }

alphaType :: Type -> Type -> P.Bool
alphaType t1 t2 = null $ alphaType' initialAlphaEnv t1 t2

alphaExpr :: Expr -> Expr -> P.Bool
alphaExpr e1 e2 = null $ alphaExpr' initialAlphaEnv e1 e2
