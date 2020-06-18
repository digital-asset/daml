-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

-- | Solving references (adding support for recursion) for DAML LF static verification
module DA.Daml.LF.Verify.ReferenceSolve
  ( solveValueReferences
  , solveChoiceReferences
  , inlineReferences
  ) where

import Data.Hashable
import Data.Maybe (fromMaybe)
import Data.Bifunctor
import qualified Data.HashMap.Strict as HM

import DA.Daml.LF.Ast hiding (lookupChoice)
import DA.Daml.LF.Verify.Subst
import DA.Daml.LF.Verify.Context
import DA.Daml.LF.Verify.Generate

-- Partially evaluate a boolean expression.
pevalBoolExpr :: (GenPhase ph, MonadEnv m ph)
  => BoolExpr
  -- ^ The expression to be analysed.
  -> m BoolExpr
pevalBoolExpr = \case
  BExpr exp -> BExpr <$> pevalExpr exp
  BAnd b1 b2 -> do
    b1' <- pevalBoolExpr b1
    b2' <- pevalBoolExpr b2
    return $ BAnd b1' b2'
  BNot b -> BNot <$> pevalBoolExpr b
  BEq b1 b2 -> do
    b1' <- pevalExpr b1
    b2' <- pevalExpr b2
    return $ BEq b1' b2'
  BGt b1 b2 -> do
    b1' <- pevalExpr b1
    b2' <- pevalExpr b2
    return $ BGt b1' b2'
  BGtE b1 b2 -> do
    b1' <- pevalExpr b1
    b2' <- pevalExpr b2
    return $ BGtE b1' b2'
  BLt b1 b2 -> do
    b1' <- pevalExpr b1
    b2' <- pevalExpr b2
    return $ BLt b1' b2'
  BLtE b1 b2 -> do
    b1' <- pevalExpr b1
    b2' <- pevalExpr b2
    return $ BLtE b1' b2'

-- | Partially evaluate an expression.
pevalExpr :: (GenPhase ph, MonadEnv m ph)
  => Expr -> m Expr
pevalExpr expr = do
  out <- genExpr False expr
  return $ _oExpr out

-- | Simplify the updates and boolean constraints in the environment, by
-- inlining value references and performing partial evaluation on the result.
inlineReferences :: MonadEnv m 'ChoiceGathering
  => m ()
inlineReferences = do
  env <- getEnv
  ctrs <- mapM pevalBoolExpr (envCtrs env)
  vals <- mapM inline_valmap (HM.toList $ envVals env)
  putEnv $ setEnvVals (HM.fromList vals) $ setEnvCtrs ctrs env
  where
    inline_valmap :: MonadEnv m 'ChoiceGathering
      => (Qualified ExprValName, (Expr, UpdateSet 'ChoiceGathering))
      -> m (Qualified ExprValName, (Expr, UpdateSet 'ChoiceGathering))
    inline_valmap (w, (e, upd)) = do
      upd' <- inline_updset upd
      return (w, (e, upd'))

    inline_updset :: MonadEnv m 'ChoiceGathering
      => UpdateSet 'ChoiceGathering
      -> m (UpdateSet 'ChoiceGathering)
    inline_updset (UpdateSetCG upds chos) = do
      upds' <- mapM inline_condupd upds
      return $ UpdateSetCG upds' chos

    inline_condupd :: MonadEnv m 'ChoiceGathering
      => Cond Upd
      -> m (Cond Upd)
    inline_condupd (Determined upd) = do
      upd' <- inline_upd upd
      return $ Determined upd'
    inline_condupd (Conditional cond xs ys) = do
      cond' <- pevalBoolExpr cond
      xs' <- mapM inline_condupd xs
      ys' <- mapM inline_condupd ys
      return $ Conditional cond' xs' ys'

    inline_upd :: MonadEnv m 'ChoiceGathering
      => Upd
      -> m Upd
    inline_upd UpdCreate{..} = do
      fields <- mapM (\(f,e) -> pevalExpr e >>= \e' -> return (f,e')) _creField
      return $ UpdCreate _creTemp fields
    inline_upd UpdArchive{..} = do
      fields <- mapM (\(f,e) -> pevalExpr e >>= \e' -> return (f,e')) _arcField
      return $ UpdCreate _arcTemp fields

-- | Solves the value references by computing the closure of all referenced
-- values, for each value in the environment.
-- It thus empties `_usValue` by collecting all updates made by this closure.
solveValueReferences :: Env 'ValueGathering -> Env 'ChoiceGathering
solveValueReferences env =
  let valhmap = foldl (\hmap ref -> snd $ solveReference lookup_ref get_refs ext_upds intro_cond empty_upds [] hmap ref) (envVals env) (HM.keys $ envVals env)
  in EnvCG (envSkols env) (convertHMap valhmap) (envDats env) (envCids env) HM.empty (envCtrs env) HM.empty
  where
    lookup_ref :: Qualified ExprValName
      -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering)
      -> (Expr, UpdateSet 'ValueGathering)
    lookup_ref ref hmap = fromMaybe (error "Impossible: Undefined value ref while solving")
      (HM.lookup ref hmap)

    get_refs :: (Expr, UpdateSet 'ValueGathering)
      -> ([Cond (Qualified ExprValName)], (Expr, UpdateSet 'ValueGathering))
    get_refs (e, upds) = (updSetValues upds, (e, setUpdSetValues [] upds))

    ext_upds :: (Expr, UpdateSet 'ValueGathering) -> (Expr, UpdateSet 'ValueGathering)
      -> (Expr, UpdateSet 'ValueGathering)
    ext_upds (e, upds1)  (_, upds2) = (e, concatUpdateSet upds1 upds2)

    intro_cond :: Cond (Expr, UpdateSet 'ValueGathering)
      -> (Expr, UpdateSet 'ValueGathering)
    -- Note that the expression is not important here, as it will be ignored in
    -- `ext_upds` later on.
    intro_cond (Determined x) = x
    intro_cond (Conditional cond cx cy) =
      let xs = map intro_cond cx
          ys = map intro_cond cy
          e = fst $ head xs
          updx = foldl concatUpdateSet emptyUpdateSet $ map snd xs
          updy = foldl concatUpdateSet emptyUpdateSet $ map snd ys
      in (e, introCond $ createCond cond updx updy)

    empty_upds :: (Expr, UpdateSet 'ValueGathering)
      -> (Expr, UpdateSet 'ValueGathering)
    empty_upds (e, _) = (e, emptyUpdateSet)

    convertHMap :: HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering)
      -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ChoiceGathering)
    convertHMap = HM.map (second updateSetVG2CG)

    updateSetVG2CG :: UpdateSet 'ValueGathering -> UpdateSet 'ChoiceGathering
    updateSetVG2CG (UpdateSetVG upd cho val)= if null val
      then UpdateSetCG upd cho
      else error "Impossible: There should be no references remaining after value solving"

-- | Solves the choice references by computing the closure of all referenced
-- choices, for each choice in the environment.
-- It thus empties `_usChoice` by collecting all updates made by this closure.
solveChoiceReferences :: Env 'ChoiceGathering -> Env 'Solving
solveChoiceReferences env =
  let chhmap = foldl (\hmap ref -> snd $ solveReference lookup_ref get_refs ext_upds intro_cond empty_upds [] hmap ref) (envChoices env) (HM.keys $ envChoices env)
      chshmap = convertChHMap chhmap
      valhmap = HM.map (inlineChoices chshmap) (envVals env)
  in EnvS (envSkols env) valhmap (envDats env) (envCids env) (envPreconds env) (envCtrs env) chshmap
  where
    lookup_ref :: UpdChoice
      -> HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering)
      -> ChoiceData 'ChoiceGathering
    lookup_ref upd hmap = fromMaybe (error "Impossible: Undefined choice ref while solving")
      (HM.lookup upd hmap)

    get_refs :: ChoiceData 'ChoiceGathering
      -> ([Cond UpdChoice], ChoiceData 'ChoiceGathering)
    get_refs chdat@ChoiceData{..} =
      let chos = updSetChoices _cdUpds
          upds = setUpdSetChoices [] _cdUpds
      in (chos, chdat{_cdUpds = upds})

    ext_upds :: ChoiceData 'ChoiceGathering
      -> ChoiceData 'ChoiceGathering
      -> ChoiceData 'ChoiceGathering
    ext_upds chdat1 chdat2 =
      let varSubst = createExprSubst [(_cdSelf chdat2, EVar (_cdSelf chdat1)), (_cdThis chdat2, EVar (_cdThis chdat1)), (_cdArgs chdat2, EVar (_cdArgs chdat1))]
          newUpds = _cdUpds chdat1 `concatUpdateSet`
            (substituteTm varSubst $ _cdUpds chdat2)
      in chdat1{_cdUpds = newUpds}

    intro_cond :: IsPhase ph
      => Cond (ChoiceData ph)
      -> ChoiceData ph
    -- Note that the expression and return type is not important here, as it
    -- will be ignored in `ext_upds` later on.
    intro_cond (Determined x) = x
    intro_cond (Conditional cond cdatxs cdatys) =
      let datxs = map intro_cond cdatxs
          datys = map intro_cond cdatys
          newUpds = introCond (createCond cond
              (foldl
                (\upd dat -> upd `concatUpdateSet` _cdUpds dat)
                emptyUpdateSet datxs)
              (foldl
                (\upd dat -> upd `concatUpdateSet` _cdUpds dat)
                emptyUpdateSet datys))
      in (head datxs){_cdUpds = newUpds}

    empty_upds :: ChoiceData 'ChoiceGathering
      -> ChoiceData 'ChoiceGathering
    empty_upds dat = dat{_cdUpds = emptyUpdateSet}

    inlineChoices :: HM.HashMap UpdChoice (ChoiceData 'Solving)
      -> (Expr, UpdateSet 'ChoiceGathering)
      -> (Expr, UpdateSet 'Solving)
    inlineChoices chshmap (exp, upds) =
      let lookupRes = map
            (intro_cond . fmap (\ch -> fromMaybe (error "Impossible: missing choice while solving") (HM.lookup ch chshmap)))
            (updSetChoices upds)
          chupds = concatMap (\ChoiceData{..} -> updSetUpdates _cdUpds) lookupRes
      in (exp, UpdateSetS (updSetUpdates upds ++ chupds))

    convertChHMap :: HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering)
      -> HM.HashMap UpdChoice (ChoiceData 'Solving)
    convertChHMap = HM.map (\chdat@ChoiceData{..} ->
      chdat{_cdUpds = updateSetCG2S _cdUpds})

    updateSetCG2S :: UpdateSet 'ChoiceGathering -> UpdateSet 'Solving
    updateSetCG2S (UpdateSetCG upd cho) = if null cho
      then UpdateSetS upd
      else error "Impossible: There should be no references remaining after choice solving"

-- | Solves a single reference by recursively inlining the references into updates.
solveReference :: forall updset ref. (Eq ref, Hashable ref)
  => (ref -> HM.HashMap ref updset -> updset)
  -- ^ Function for looking up references in the update set.
  -> (updset -> ([Cond ref], updset))
  -- ^ Function popping the references from the update set.
  -> (updset -> updset -> updset)
  -- ^ Function for concatinating update sets.
  -> (Cond updset -> updset)
  -- ^ Function for moving conditionals inside the update set.
  -> (updset -> updset)
  -- ^ Function for emptying a given update set of all updates.
  -> [ref]
  -- ^ The references which have already been visited.
  -> HM.HashMap ref updset
  -- ^ The hashmap mapping references to update sets.
  -> ref
  -- ^ The reference to be solved.
  -> (updset, HM.HashMap ref updset)
solveReference lookup getRefs extUpds introCond emptyUpds vis hmap0 ref0 =
  -- Lookup updates performed by the given reference, and split in new
  -- references and reference-free updates.
  let upd0 = lookup ref0 hmap0
      (refs, upd1) = getRefs upd0
  -- Check for loops. If the references has already been visited, then the
  -- reference should be flagged as recursive.
  in if ref0 `elem` vis
  -- TODO: Recursion!
    then (upd1, hmap0) -- TODO: At least remove the references?
    -- When no recursion has been detected, continue inlining the references.
    else let (upd2, hmap1) = foldl handle_ref (upd1, hmap0) refs
      in (upd1, HM.insert ref0 upd2 hmap1)
  where
    -- | Extend the closure by computing and adding the reference closure for
    -- the given reference.
    handle_ref :: (updset, HM.HashMap ref updset)
      -- ^ The current closure (update set) and the current map for reference to update.
      -> Cond ref
      -- ^ The reference to be computed and added.
      -> (updset, HM.HashMap ref updset)
    -- For a simple reference, the closure is computed straightforwardly.
    handle_ref (upd_i0, hmap_i0) (Determined ref_i) =
      let (upd_i1, hmap_i1) =
            solveReference lookup getRefs extUpds introCond emptyUpds (ref0:vis) hmap_i0 ref_i
      in (extUpds upd_i0 upd_i1, hmap_i1)
    -- A conditional reference is more involved, as the conditional needs to be
    -- preserved in the computed closure (update set).
    handle_ref (upd_i0, hmap_i0) (Conditional cond refs_ia refs_ib) =
          -- Construct an update set without any updates.
      let upd_i0_empty = emptyUpds upd_i0
          -- Compute the closure for the true-case.
          (upd_ia, hmap_ia) = foldl handle_ref (upd_i0_empty, hmap_i0) refs_ia
          -- Compute the closure for the false-case.
          (upd_ib, hmap_ib) = foldl handle_ref (upd_i0_empty, hmap_ia) refs_ib
          -- Move the conditional inwards, in the update set.
          upd_i1 = extUpds upd_i0 $ introCond $ createCond cond upd_ia upd_ib
      in (upd_i1, hmap_ib)
