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
import Data.List (intersperse)
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

    -- TODO: This whole thing should really just be a monadic fmap.
    inline_updset :: MonadEnv m 'ChoiceGathering
      => UpdateSet 'ChoiceGathering
      -> m (UpdateSet 'ChoiceGathering)
    inline_updset (UpdateSetCG upds chos) = do
      upds' <- mapM inline_recupd upds
      return $ UpdateSetCG upds' chos

    inline_recupd :: MonadEnv m 'ChoiceGathering
      => Rec [Cond Upd]
      -> m (Rec [Cond Upd])
    inline_recupd (Simple x) = do
      x' <- mapM inline_condupd x
      return $ Simple x'
    inline_recupd (Rec xs) = do
      xs' <- mapM (mapM inline_condupd) xs
      return $ Rec xs'
    inline_recupd (MutRec xs) = do
      xs' <- mapM (\(s,ys) -> mapM inline_condupd ys >>= \ys' -> return (s,ys')) xs
      return $ MutRec xs'

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
  let (_, valhmap) = foldl (\hmaps ref -> snd $ solveReference lookup_ref_in lookup_ref_out get_refs ext_upds make_rec make_mutrec intro_cond empty_upds [] hmaps ref) (envVals env, HM.empty) (HM.keys $ envVals env)
  in EnvCG (envSkols env) valhmap (envDats env) (envCids env) HM.empty (envCtrs env) HM.empty
  where
    lookup_ref_in :: Qualified ExprValName
      -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering)
      -> Maybe ( (Expr, UpdateSet 'ValueGathering)
               , HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ValueGathering) )
    lookup_ref_in ref hmap = (\upd -> (upd, HM.delete ref hmap)) <$> (HM.lookup ref hmap)

    lookup_ref_out :: Qualified ExprValName
      -> HM.HashMap (Qualified ExprValName) (Expr, UpdateSet 'ChoiceGathering)
      -> Maybe (Expr, UpdateSet 'ChoiceGathering)
    lookup_ref_out ref hmap = HM.lookup ref hmap

    get_refs :: (Expr, UpdateSet 'ValueGathering)
      -> ([Cond (Qualified ExprValName)], (Expr, UpdateSet 'ChoiceGathering))
    get_refs (e, UpdateSetVG upds chos vals) =
      let upds' = [Simple upds]
      in (vals, (e, UpdateSetCG upds' chos))

    ext_upds :: (Expr, UpdateSet 'ChoiceGathering) -> (Expr, UpdateSet 'ChoiceGathering)
      -> (Expr, UpdateSet 'ChoiceGathering)
    ext_upds (e, upds1)  (_, upds2) = (e, concatUpdateSet upds1 upds2)

    make_rec :: (Expr, UpdateSet 'ChoiceGathering) -> (Expr, UpdateSet 'ChoiceGathering)
    make_rec (e, upds) = (e, setUpdSetUpdates (makeRec (updSetUpdates upds)) upds)

    make_mutrec :: [(Qualified ExprValName, (Expr, UpdateSet 'ChoiceGathering))]
      -> (Expr, UpdateSet 'ChoiceGathering)
    make_mutrec inp =
      let (strs, upds) = unzip inp
          debug = concat $ intersperse " - " $ map (show . unExprValName . qualObject) strs
          updConcat = foldl concatUpdateSet emptyUpdateSet $ map snd upds
      in (fst $ head upds, setUpdSetUpdates (makeMutRec (updSetUpdates updConcat) debug) updConcat)

    intro_cond :: Cond (Expr, UpdateSet 'ChoiceGathering)
      -> (Expr, UpdateSet 'ChoiceGathering)
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

    empty_upds :: (Expr, UpdateSet 'ChoiceGathering)
      -> (Expr, UpdateSet 'ChoiceGathering)
    empty_upds (e, _) = (e, emptyUpdateSet)

-- | Solves the choice references by computing the closure of all referenced
-- choices, for each choice in the environment.
-- It thus empties `_usChoice` by collecting all updates made by this closure.
solveChoiceReferences :: Env 'ChoiceGathering -> Env 'Solving
solveChoiceReferences env =
  let (_, chhmap) = foldl (\hmaps ref -> snd $ solveReference lookup_ref_in lookup_ref_out get_refs ext_upds make_rec make_mutrec intro_cond empty_upds [] hmaps ref) (envChoices env, HM.empty) (HM.keys $ envChoices env)
      valhmap = HM.map (inlineChoices chhmap) (envVals env)
  in EnvS (envSkols env) valhmap (envDats env) (envCids env) (envPreconds env) (envCtrs env) chhmap
  where
    lookup_ref_in :: UpdChoice
      -> HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering)
      -> Maybe ( ChoiceData 'ChoiceGathering
               , HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering) )
    lookup_ref_in ref hmap = (\upd -> (upd, HM.delete ref hmap)) <$> (HM.lookup ref hmap)

    lookup_ref_out :: UpdChoice
      -> HM.HashMap UpdChoice (ChoiceData 'Solving)
      -> Maybe (ChoiceData 'Solving)
    lookup_ref_out ref hmap = HM.lookup ref hmap

    get_refs :: ChoiceData 'ChoiceGathering
      -> ([Cond UpdChoice], ChoiceData 'Solving)
    get_refs chdat@ChoiceData{..} =
      let chos = updSetChoices _cdUpds
          upds = UpdateSetS $ updSetUpdates _cdUpds
      in (chos, chdat{_cdUpds = upds})

    ext_upds :: ChoiceData 'Solving
      -> ChoiceData 'Solving
      -> ChoiceData 'Solving
    ext_upds chdat1 chdat2 =
      let varSubst = createExprSubst [(_cdSelf chdat2, EVar (_cdSelf chdat1)), (_cdThis chdat2, EVar (_cdThis chdat1)), (_cdArgs chdat2, EVar (_cdArgs chdat1))]
          newUpds = _cdUpds chdat1 `concatUpdateSet`
            (substituteTm varSubst $ _cdUpds chdat2)
      in chdat1{_cdUpds = newUpds}

    make_rec :: ChoiceData 'Solving -> ChoiceData 'Solving
    make_rec chdat@ChoiceData{..} =
      let upds = setUpdSetUpdates (makeRec (updSetUpdates _cdUpds)) _cdUpds
      in chdat{_cdUpds = upds}

    make_mutrec :: [(UpdChoice, ChoiceData 'Solving)] -> ChoiceData 'Solving
    make_mutrec inp =
      let (strs, chdats) = unzip inp
          debug = concat $ intersperse " - " $ map (show . unChoiceName . _choName) strs
          chdat = concat_chdats chdats
          upds = setUpdSetUpdates (makeMutRec (updSetUpdates $ _cdUpds chdat) debug) $ _cdUpds chdat
      in chdat{_cdUpds = upds}

    -- | Internal function for combining choice data's. Note that the return
    -- type is considered to be irrelevant here, and the first one is returned
    -- arbitrarily.
    -- Also note that the input list is expected to be non-empty.
    concat_chdats :: IsPhase ph => [ChoiceData ph] -> ChoiceData ph
    concat_chdats inp =
      let chdat = head inp
          upds = foldl concatUpdateSet emptyUpdateSet $ map _cdUpds inp
      in chdat{_cdUpds = upds}

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

    empty_upds :: ChoiceData 'Solving
      -> ChoiceData 'Solving
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

-- | Solves a single reference by recursively inlining the references into updates.
solveReference :: forall ref updset0 updset1. (Eq ref, Hashable ref, Show ref)
  => (ref -> HM.HashMap ref updset0 -> Maybe (updset0, HM.HashMap ref updset0))
  -- ^ Function for looking up and popping a reference from the update set.
  -> (ref -> HM.HashMap ref updset1 -> Maybe updset1)
  -- ^ Function for looking up a reference in the solved update set.
  -> (updset0 -> ([Cond ref], updset1))
  -- ^ Function popping the references from the update set.
  -> (updset1 -> updset1 -> updset1)
  -- ^ Function for concatinating update sets.
  -> (updset1 -> updset1)
  -- ^ Function for marking an update set as recursive.
  -> ([(ref, updset1)] -> updset1)
  -- ^ Function for combining and marking a list of update sets as mutually recursive.
  -> (Cond updset1 -> updset1)
  -- ^ Function for moving conditionals inside the update set.
  -> (updset1 -> updset1)
  -- ^ Function for emptying a given update set of all updates.
  -> [(ref, updset1)]
  -- TODO: HashMap?
  -- ^ The references which have already been visited.
  -> ( HM.HashMap ref updset0
  -- ^ The hashmap mapping references to update sets and references.
  -- These still need to be solved.
     , HM.HashMap ref updset1 )
  -- ^ The hashmap mapping references to solved update sets (without references).
  -- This starts out empty.
  -> ref
  -- ^ The reference to be solved.
  -> (updset1, (HM.HashMap ref updset0, HM.HashMap ref updset1))
solveReference lookupRef lookupSol getRefs extUpds makeRec makeMutRec introCond emptyUpds vis (hmapRef0, hmapSol0) ref0 =
  -- Check for loops. If the references has already been visited, then the
  -- reference should be flagged as recursive.
  case snd $ break (\(ref,_) -> ref == ref0) vis of

    -- When no recursion has been detected, continue inlining the references.
    -- TODO: This has to be in order, so no updates are missed in the rec cycle.
    -- First, lookup the update set for the given reference, and remove it from
    -- the hashmap.
    [] -> case lookupRef ref0 hmapRef0 of
      -- The reference has already been solved, and can just be returned.
      Nothing -> case lookupSol ref0 hmapSol0 of
        Nothing -> error "Impossible: Unknown reference during solving "
        Just upd0 -> (upd0, (hmapRef0, hmapSol0))
      -- The reference has not yet been solved.
      Just (upd0, hmapRef1) ->
        -- Split the update set in references and reference-free updates.
        let (refs, upd1) = getRefs upd0
            (upd2, (hmapRef2, hmapSol2)) = foldl handle_ref (upd1, (hmapRef1, hmapSol0)) refs
        in (upd2, (hmapRef2, HM.insert ref0 upd2 hmapSol2))

    -- When recursion has been detected, halt solving and mark the cycle as
    -- recursive.
    [(_cho,upd2)] -> let upd3 = makeRec upd2
      in (upd3, (hmapRef0, HM.insert ref0 upd3 hmapSol0))

    -- When mutual recursion has been detected, halt solving and mark all cycles
    -- as mutually recursive.
    cycle -> let upd2 = makeMutRec cycle
      in (upd2, (hmapRef0, HM.insert ref0 upd2 hmapSol0))
  where
    -- | Extend the closure by computing and adding the reference closure for
    -- the given reference.
    handle_ref :: (updset1, (HM.HashMap ref updset0, HM.HashMap ref updset1))
      -- ^ The current closure (update set) and the current map for reference to update.
      -> Cond ref
      -- ^ The reference to be computed and added.
      -> (updset1, (HM.HashMap ref updset0, HM.HashMap ref updset1))
    -- For a simple reference, the closure is computed straightforwardly.
    handle_ref (upd_i0, hmaps_i0) (Determined ref_i) =
      let (upd_i1, hmaps_i1) =
            solveReference lookupRef lookupSol getRefs extUpds makeRec makeMutRec introCond emptyUpds (vis ++ [(ref0,upd_i0)]) hmaps_i0 ref_i
      in (extUpds upd_i0 upd_i1, hmaps_i1)
    -- A conditional reference is more involved, as the conditional needs to be
    -- preserved in the computed closure (update set).
    handle_ref (upd_i0, hmaps_i0) (Conditional cond refs_ia refs_ib) =
          -- Construct an update set without any updates.
      let upd_i0_empty = emptyUpds upd_i0
          -- Compute the closure for the true-case.
          (upd_ia, hmaps_ia) = foldl handle_ref (upd_i0, hmaps_i0) refs_ia
          -- Compute the closure for the false-case.
          (upd_ib, hmaps_ib) = foldl handle_ref (upd_i0, hmaps_ia) refs_ib
          -- Move the conditional inwards, in the update set.
          upd_i1 = extUpds upd_i0_empty $ introCond $ createCond cond upd_ia upd_ib
          -- TODO: This has the unfortunate side effect of moving all updates inside the conditional.
      in (upd_i1, hmaps_ib)
