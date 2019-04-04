-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.Type
  ( freeVars
  , alphaEquiv
  , Subst
  , substitute
  ) where

import DA.Prelude

import           Data.Bifunctor
import           Data.Functor.Foldable (cata)
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import           Safe (findJust)
import           Safe.Exact (zipWithExactMay)

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Recursive

-- | Get the free type variables of a type.
freeVars :: Type -> Set.Set TypeVarName
freeVars = cata go
  where
    go = \case
      TVarF v -> Set.singleton v
      TConF _ -> mempty
      TAppF s1 s2 -> s1 <> s2
      TBuiltinF _ -> mempty
      TForallF (v, _k) s -> v `Set.delete` s
      TTupleF fs -> foldMap snd fs

-- | Auxiliary data structure to track bound variables in the test for alpha
-- equivalence.
data AlphaEnv = AlphaEnv
  { currentDepth :: !Int
    -- ^ Current quantifier depth.
  , binderDepthLhs :: !(Map.Map TypeVarName Int)
    -- ^ Maps bound type variables from the left-hand-side of 'alphaEquiv' to
    -- the depth of the quantifier which introduced them.
  , binderDepthRhs :: !(Map.Map TypeVarName Int)
    -- ^ Maps bound type variables from the right-hand-side of 'alphaEquiv' to
    -- the depth of the quantifier which introduced them.
  }

-- | Test two types for alpha equivalence.
alphaEquiv :: Type -> Type -> Bool
alphaEquiv = go (AlphaEnv 0 Map.empty Map.empty)
  where
    go :: AlphaEnv -> Type -> Type -> Bool
    go env0@AlphaEnv{currentDepth, binderDepthLhs, binderDepthRhs} = curry $ \case
      (TVar v1, TVar v2) ->
        case (Map.lookup v1 binderDepthLhs, Map.lookup v2 binderDepthRhs) of
          (Just l1, Just l2) -> l1 == l2
          (Nothing, Nothing) -> v1 == v2
          (Just _ , Nothing) -> False
          (Nothing, Just _ ) -> False
      (TCon c1, TCon c2) -> c1 == c2
      (TApp tf1 ta1, TApp tf2 ta2) -> go0 tf1 tf2 && go0 ta1 ta2
      (TBuiltin b1, TBuiltin b2) -> b1 == b2
      (TForall (v1, k1) t1, TForall (v2, k2) t2) ->
        let env1 = AlphaEnv
              { currentDepth   = currentDepth + 1
              , binderDepthLhs = Map.insert v1 currentDepth binderDepthLhs
              , binderDepthRhs = Map.insert v2 currentDepth binderDepthRhs
              }
        in  k1 == k2 && go env1 t1 t2
      (TTuple fs1, TTuple fs2)
        | Just bs <- zipWithExactMay agree (sortOn fst fs1) (sortOn fst fs2) ->
            and bs
        where
          agree (l1, t1) (l2, t2) = l1 == l2 && go0 t1 t2
      (_, _) -> False
      where
        go0 = go env0

-- | Substitution of types for type variables.
type Subst = Map.Map TypeVarName Type

-- | Capture-avoiding substitution. It operates under the following assumption:
-- If a type variable is free in the substitution and free in the type we're
-- substituting into but not contained in the domain of the substitution, then
-- both free occurrences refer to the same binder.
substitute :: Subst -> Type -> Type
substitute subst = go (foldMap freeVars subst) subst
  where
    -- NOTE(MH): We maintain the invariant that @fvSubst0@ contains the free
    -- variables of @subst0@ or an over-approximation thereof.
    go :: Set.Set TypeVarName -> Subst -> Type -> Type
    go fvSubst0 subst0 = \case
      TVar v
        | Just t <- Map.lookup v subst0 -> t
        | otherwise                     -> TVar v
      TCon c -> TCon c
      TApp t1 t2 -> TApp (go0 t1) (go0 t2)
      TBuiltin b -> TBuiltin b
      TForall (v0, k) t
        | v0 `Set.member` fvSubst0 ->
            let v1 = freshTypeVar fvSubst0 v0
                fvSubst1 = Set.insert v1 fvSubst0
                subst1 = Map.insert v0 (TVar v1) subst0
            in  TForall (v1, k) (go fvSubst1 subst1 t)
        | otherwise -> TForall (v0, k) (go fvSubst0 (Map.delete v0 subst0) t)
      TTuple fs -> TTuple (map (second go0) fs)
      where
        go0 = go fvSubst0 subst0

-- | Generate a fresh type variables that is not contained in the given set.
-- Uses another type variable as a starting point for the search for a fresh
-- name.
freshTypeVar :: Set.Set TypeVarName -> TypeVarName -> TypeVarName
freshTypeVar s (Tagged v) =
  let candidates = map (\n -> Tagged (v <> T.pack (show n))) [1 :: Int ..]
  in  findJust (`Set.notMember` s) candidates
