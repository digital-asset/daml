-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.Type
  ( freeVars
  , referencedSyns
  , alphaEquiv
  , Subst
  , substitute
  ) where

import           Data.Bifunctor
import qualified Data.HashSet as HS
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.List
import           Safe (findJust)
import           Safe.Exact (zipWithExactMay)

import DA.Daml.LF.Ast.Base

-- | Get the free type variables of a type.
freeVars :: Type -> Set.Set TypeVarName
freeVars e = go Set.empty e Set.empty
  where
    go !boundVars e !acc = case e of
        TVar v
            | v `Set.member` boundVars -> acc
            | otherwise -> Set.insert v acc
        TCon _ -> acc
        TSynApp _ args -> foldl' (\acc t -> go boundVars t acc) acc args
        TApp s1 s2 -> go boundVars s1 $ go boundVars s2 acc
        TBuiltin _ -> acc
        TForall (v, _k) s -> go (Set.insert v boundVars) s acc
        TStruct fs -> foldl' (\acc (_, t) -> go boundVars t acc) acc fs
        TNat _ -> acc

-- | Get the type synonyms referenced by a type.
referencedSyns :: Type -> HS.HashSet (Qualified TypeSynName)
referencedSyns = go HS.empty
  where
    go !acc = \case
      TVar _ -> acc
      TCon _ -> acc
      TSynApp qsyn args -> foldl' go (HS.insert qsyn acc) args
      TApp s1 s2 -> go (go acc s1) s2
      TBuiltin _ -> acc
      TForall _ body -> go acc body
      TStruct fs -> foldl' go acc (map snd fs)
      TNat _ -> acc

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
      (TStruct fs1, TStruct fs2)
        | Just bs <- zipWithExactMay agree (sortOn fst fs1) (sortOn fst fs2) ->
            and bs
        where
          agree (l1, t1) (l2, t2) = l1 == l2 && go0 t1 t2
      (TNat n1, TNat n2) -> n1 == n2
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
substitute subst = go (Map.foldl' (\acc t -> acc `Set.union` freeVars t) Set.empty subst) subst
  where
    -- NOTE(MH): We maintain the invariant that @fvSubst0@ contains the free
    -- variables of @subst0@ or an over-approximation thereof.
    go :: Set.Set TypeVarName -> Subst -> Type -> Type
    go fvSubst0 subst0 = \case
      TVar v
        | Just t <- Map.lookup v subst0 -> t
        | otherwise                     -> TVar v
      TCon c -> TCon c
      TSynApp s args -> TSynApp s (map go0 args)
      TApp t1 t2 -> TApp (go0 t1) (go0 t2)
      TBuiltin b -> TBuiltin b
      TForall (v0, k) t
        | v0 `Set.member` fvSubst0 ->
            let v1 = freshTypeVar fvSubst0 v0
                fvSubst1 = Set.insert v1 fvSubst0
                subst1 = Map.insert v0 (TVar v1) subst0
            in  TForall (v1, k) (go fvSubst1 subst1 t)
        | otherwise ->
            let fvSubst1 = Set.insert v0 fvSubst0
            in TForall (v0, k) (go fvSubst1 (Map.delete v0 subst0) t)
      TStruct fs -> TStruct (map (second go0) fs)
      TNat n -> TNat n
      where
        go0 = go fvSubst0 subst0

-- | Generate a fresh type variables that is not contained in the given set.
-- Uses another type variable as a starting point for the search for a fresh
-- name.
freshTypeVar :: Set.Set TypeVarName -> TypeVarName -> TypeVarName
freshTypeVar s (TypeVarName v) =
  let candidates = map (\n -> TypeVarName (v <> T.pack (show n))) [1 :: Int ..]
  in  findJust (`Set.notMember` s) candidates
