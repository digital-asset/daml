-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

-- | Solving references (adding support for recursion) for DAML LF static verification
module DA.Daml.LF.Verify.ReferenceSolve
  ( solveValueReferences
  , solveChoiceReferences
  ) where

import Data.Hashable
import Data.Maybe (fromMaybe)
import Data.List (foldl', intercalate)
import qualified Data.HashMap.Strict as HM

import DA.Daml.LF.Ast hiding (lookupChoice)
import DA.Daml.LF.Ast.Subst
import DA.Daml.LF.Verify.Context

-- | Solves the value references by computing the closure of all referenced
-- values, for each value in the environment.
-- It thus empties `_usValue` by collecting all updates made by this closure.
solveValueReferences :: Env 'ValueGathering -> Env 'ChoiceGathering
solveValueReferences env =
  let val_exp_hmap = HM.map fst $ envVals env
      val_ref_hmap = HM.map snd $ envVals env
      (_, val_sol_hmap) = foldl' (\hmaps ref -> snd $ solveReference lookup_ref_in lookup_ref_out pop_upds ext_upds make_rec make_mutrec intro_cond empty_upds [] hmaps ref) (val_ref_hmap, HM.empty) (HM.keys $ envVals env)
      valhmap = HM.intersectionWith (\e u -> (e,u)) val_exp_hmap val_sol_hmap
  in EnvCG (envSkols env) valhmap (envDats env) (envCids env) HM.empty (envCtrs env) HM.empty
  where
    lookup_ref_in :: Qualified ExprValName
      -> HM.HashMap (Qualified ExprValName) (UpdateSet 'ValueGathering)
      -> Maybe ( UpdateSet 'ValueGathering
               , HM.HashMap (Qualified ExprValName) (UpdateSet 'ValueGathering) )
    lookup_ref_in ref hmap = (, HM.delete ref hmap) <$> HM.lookup ref hmap

    lookup_ref_out :: Qualified ExprValName
      -> HM.HashMap (Qualified ExprValName) (UpdateSet 'ChoiceGathering)
      -> Maybe (UpdateSet 'ChoiceGathering)
    lookup_ref_out ref hmap = HM.lookup ref hmap

    pop_upds :: UpdateSet 'ValueGathering
      -> Maybe ( Either (Cond (Qualified ExprValName))
                        (UpdateSet 'ChoiceGathering)
               , UpdateSet 'ValueGathering )
    pop_upds = \case
      [] -> Nothing
      (upd:updset1) ->
        let upd' = case upd of
              UpdVGBase base -> Right [UpdCGBase $ Simple base]
              UpdVGChoice cho -> Right [UpdCGChoice cho]
              UpdVGVal val -> Left val
        in Just (upd', updset1)

    ext_upds :: UpdateSet 'ChoiceGathering -> UpdateSet 'ChoiceGathering
      -> UpdateSet 'ChoiceGathering
    ext_upds = concatUpdateSet

    make_rec :: UpdateSet 'ChoiceGathering -> UpdateSet 'ChoiceGathering
    make_rec upds = (map baseUpd $ makeRec [upd | UpdCGBase upd <- upds])
      ++ [UpdCGChoice cho | UpdCGChoice cho <- upds]

    make_mutrec :: [(Qualified ExprValName, UpdateSet 'ChoiceGathering)]
      -> UpdateSet 'ChoiceGathering
    make_mutrec inp =
      let (strs, upds) = unzip inp
          debug = intercalate " - " $ map (show . unExprValName . qualObject) strs
          updConcat = foldl' concatUpdateSet emptyUpdateSet upds
      in (map baseUpd $ makeMutRec [upd | UpdCGBase upd <- updConcat] debug)
           ++ [UpdCGChoice cho | UpdCGChoice cho <- updConcat]

    intro_cond :: Cond (UpdateSet 'ChoiceGathering)
      -> UpdateSet 'ChoiceGathering
    intro_cond (Determined x) = x
    intro_cond (Conditional cond cx cy) =
      let xs = map intro_cond cx
          ys = map intro_cond cy
          updx = foldl' concatUpdateSet emptyUpdateSet xs
          updy = foldl' concatUpdateSet emptyUpdateSet ys
      in (introCond $ createCond cond updx updy)

    empty_upds :: UpdateSet 'ValueGathering
      -> UpdateSet 'ChoiceGathering
    empty_upds _ = emptyUpdateSet

-- | Solves the choice references by computing the closure of all referenced
-- choices, for each choice in the environment.
-- It thus empties `_usChoice` by collecting all updates made by this closure.
solveChoiceReferences :: Env 'ChoiceGathering -> Env 'Solving
solveChoiceReferences env =
  let (_, chhmap) = foldl' (\hmaps ref -> snd $ solveReference lookup_ref_in lookup_ref_out pop_upds ext_upds make_rec make_mutrec intro_cond empty_upds [] hmaps ref) (envChoices env, HM.empty) (HM.keys $ envChoices env)
      valhmap = HM.map (inlineChoices chhmap) (envVals env)
  in EnvS (envSkols env) valhmap (envDats env) (envCids env) (envPreconds env) (envCtrs env) chhmap
  where
    lookup_ref_in :: UpdChoice
      -> HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering)
      -> Maybe ( ChoiceData 'ChoiceGathering
               , HM.HashMap UpdChoice (ChoiceData 'ChoiceGathering) )
    lookup_ref_in ref hmap = (, HM.delete ref hmap) <$> HM.lookup ref hmap

    lookup_ref_out :: UpdChoice
      -> HM.HashMap UpdChoice (ChoiceData 'Solving)
      -> Maybe (ChoiceData 'Solving)
    lookup_ref_out ref hmap = HM.lookup ref hmap

    pop_upds :: ChoiceData 'ChoiceGathering
      -> Maybe ( Either (Cond UpdChoice) (ChoiceData 'Solving)
               , ChoiceData 'ChoiceGathering )
    pop_upds chdat@ChoiceData{..} = case _cdUpds of
      [] -> Nothing
      (upd:upds) ->
        let upd' = case upd of
              UpdCGBase base -> Right chdat{_cdUpds = [UpdSBase base]}
              UpdCGChoice cho -> Left cho
        in Just (upd', chdat{_cdUpds = upds})

    ext_upds :: ChoiceData 'Solving
      -> ChoiceData 'Solving
      -> ChoiceData 'Solving
    ext_upds chdat1 chdat2 =
      let varSubst = foldMap (uncurry exprSubst) [(_cdSelf chdat2, EVar (_cdSelf chdat1)), (_cdThis chdat2, EVar (_cdThis chdat1)), (_cdArgs chdat2, EVar (_cdArgs chdat1))]
          newUpds = _cdUpds chdat1 `concatUpdateSet`
            map (applySubstInUpd varSubst) (_cdUpds chdat2)
      in chdat1{_cdUpds = newUpds}

    make_rec :: ChoiceData 'Solving -> ChoiceData 'Solving
    make_rec chdat@ChoiceData{..} =
      let upds = map baseUpd $ makeRec [upd | UpdSBase upd <- _cdUpds]
      in chdat{_cdUpds = upds}

    make_mutrec :: [(UpdChoice, ChoiceData 'Solving)] -> ChoiceData 'Solving
    make_mutrec inp =
      let (strs, chdats) = unzip inp
          debug = intercalate " - " $ map (show . unChoiceName . _choName) strs
          chdat = concat_chdats chdats
          upds = map baseUpd $ makeMutRec [upd | UpdSBase upd <- _cdUpds chdat] debug
      in chdat{_cdUpds = upds}

    -- | Internal function for combining choice data's. Note that the return
    -- type is considered to be irrelevant here, and the first one is returned
    -- arbitrarily.
    -- Also note that the input list is expected to be non-empty.
    concat_chdats :: IsPhase ph => [ChoiceData ph] -> ChoiceData ph
    concat_chdats inp =
      let chdat = head inp
          upds = foldl' concatUpdateSet emptyUpdateSet $ map _cdUpds inp
      in chdat{_cdUpds = upds}

    intro_cond :: IsPhase ph
      => Cond (ChoiceData ph)
      -> ChoiceData ph
    intro_cond (Determined x) = x
    intro_cond (Conditional cond cdatxs cdatys) =
      let datxs = map intro_cond cdatxs
          datys = map intro_cond cdatys
          newUpds = introCond (createCond cond
              (foldl'
                (\upd dat -> upd `concatUpdateSet` _cdUpds dat)
                emptyUpdateSet datxs)
              (foldl'
                (\upd dat -> upd `concatUpdateSet` _cdUpds dat)
                emptyUpdateSet datys))
      in (head datxs){_cdUpds = newUpds}

    empty_upds :: ChoiceData 'ChoiceGathering
      -> ChoiceData 'Solving
    empty_upds dat = dat{_cdUpds = emptyUpdateSet}

    inlineChoices :: HM.HashMap UpdChoice (ChoiceData 'Solving)
      -> (Expr, UpdateSet 'ChoiceGathering)
      -> (Expr, UpdateSet 'Solving)
    inlineChoices chshmap (exp, upds) =
      (exp, concatMap inline_choice upds)
      where
        inline_choice :: Upd 'ChoiceGathering -> UpdateSet 'Solving
        inline_choice (UpdCGBase upd) = [UpdSBase upd]
        inline_choice (UpdCGChoice (Determined cho)) =
          let chdat = fromMaybe (error "Impossible: missing choice while solving") (HM.lookup cho chshmap)
          in _cdUpds chdat
        inline_choice (UpdCGChoice (Conditional cond chos_a chos_b)) =
          let upds_a = map (Determined . inline_choice . UpdCGChoice) chos_a
              upds_b = map (Determined . inline_choice . UpdCGChoice) chos_b
          in introCond $ Conditional cond upds_a upds_b

-- | Solves a single reference by recursively inlining the references into updates.
solveReference :: forall ref updset0 updset1. (Eq ref, Hashable ref, Show ref)
  => (ref -> HM.HashMap ref updset0 -> Maybe (updset0, HM.HashMap ref updset0))
  -- ^ Function for looking up and popping a reference from the update set.
  -> (ref -> HM.HashMap ref updset1 -> Maybe updset1)
  -- ^ Function for looking up a reference in the solved update set.
  -> (updset0 -> Maybe (Either (Cond ref) updset1 , updset0))
  -- ^ Function for popping an element from an update set. Returns Nothing if the
  -- update set is empty. Returns a Left value is the element is a reference, or
  -- Right if it's an update.
  -> (updset1 -> updset1 -> updset1)
  -- ^ Function for concatinating update sets.
  -> (updset1 -> updset1)
  -- ^ Function for marking an update set as recursive.
  -> ([(ref, updset1)] -> updset1)
  -- ^ Function for combining and marking a list of update sets as mutually recursive.
  -> (Cond updset1 -> updset1)
  -- ^ Function for moving conditionals inside the update set.
  -> (updset0 -> updset1)
  -- ^ Function for emptying a given update set of all updates.
  -> [(ref, updset1)]
  -- ^ The references which have already been visited. Note that this a list
  -- rather than a HashMap, as the ordering is important here.
  -> ( HM.HashMap ref updset0
  -- ^ The hashmap mapping references to update sets and references.
  -- These still need to be solved.
     , HM.HashMap ref updset1 )
  -- ^ The hashmap mapping references to solved update sets (without references).
  -- This starts out empty.
  -> ref
  -- ^ The reference to be solved.
  -> (updset1, (HM.HashMap ref updset0, HM.HashMap ref updset1))
solveReference lookupRef lookupSol popUpd extUpds makeRec makeMutRec introCond emptyUpds vis (hmapRef0, hmapSol0) ref0 =
  -- Check for loops. If the references has already been visited, then the
  -- reference should be flagged as recursive.
  case dropWhile (not . (\(ref,_) -> ref == ref0)) vis of

    -- When no recursion has been detected, continue inlining the references.
    -- First, lookup the update set for the given reference, and remove it from
    -- the hashmap.
    [] -> case lookupRef ref0 hmapRef0 of
      -- The reference has already been solved, and can just be returned.
      Nothing -> case lookupSol ref0 hmapSol0 of
        Nothing -> error "Impossible: Unknown reference during solving "
        Just upd0 -> (upd0, (hmapRef0, hmapSol0))
      -- The reference has not yet been solved.
      Just (upd0, hmapRef1) ->
        -- Resolve the references contained in the update set.
        let (upd1, (hmapRef2, hmapSol2)) = handle_upds upd0 (emptyUpds upd0) (hmapRef1, hmapSol0)
        in (upd1, (hmapRef2, HM.insert ref0 upd1 hmapSol2))

    -- When recursion has been detected, halt solving and mark the cycle as
    -- recursive.
    [(_cho,upd2)] -> let upd3 = makeRec upd2
      in (upd3, (hmapRef0, HM.insert ref0 upd3 hmapSol0))

    -- When mutual recursion has been detected, halt solving and mark all cycles
    -- as mutually recursive.
    cycle -> let upd2 = makeMutRec cycle
      in (upd2, (hmapRef0, HM.insert ref0 upd2 hmapSol0))
  where
    -- | Extend the closure by computing the closure over all references in the
    -- given update set.
    -- Returns the handled update set (no longer contains references), and the
    -- altered hash maps.
    handle_upds :: updset0
      -- ^ The update set for the current reference that still needs to be
      -- handled. This still contains references to solve.
      -> updset1
      -- ^ The update set of the current reference that has already been handled.
      -> (HM.HashMap ref updset0, HM.HashMap ref updset1)
      -- ^ The maps containing the yet to be solved references and the
      -- previously solved references, respectively.
      -> (updset1, (HM.HashMap ref updset0, HM.HashMap ref updset1))
    handle_upds updset_ref0 updset_sol0 hmaps0 = case popUpd updset_ref0 of
      -- When the update set to handle is empty, return.
      Nothing -> (updset_sol0, hmaps0)
      -- When the update set is non-empty, handle the first entry.
      Just (entry, updset_ref_rem) -> case entry of
        -- The update the handle is a reference.
        Left ref ->
          let vis' = vis ++ [(ref0,updset_sol0)]
              (updset_sol1, hmaps1) = handle_ref vis' (updset_sol0, hmaps0) ref
          in handle_upds updset_ref_rem updset_sol1 hmaps1
        -- The update to handle is a base update.
        Right upds -> handle_upds updset_ref_rem (extUpds updset_sol0 upds) hmaps0

    -- | Extend the closure by resolving a single given reference.
    handle_ref :: [(ref,updset1)]
      -- ^ The list of previously visited references.
      -> (updset1, (HM.HashMap ref updset0, HM.HashMap ref updset1))
      -- ^ The update set of the current reference that has already been handled,
      -- along with the maps containing the yet to be solved references and the
      -- previously solved references, respectively.
      -> Cond ref
      -- ^ The reference to be resolved and added.
      -> (updset1, (HM.HashMap ref updset0, HM.HashMap ref updset1))
    handle_ref vis (updset0, hmaps0) = \case
      -- For a simple reference, the closure is computed straightforwardly.
      Determined ref ->
        let (upds_ext, hmaps1) = solveReference lookupRef lookupSol popUpd extUpds makeRec makeMutRec introCond emptyUpds vis hmaps0 ref
        in (extUpds updset0 upds_ext, hmaps1)
      -- A conditional reference is more involved, as the conditional needs to be
      -- preserved in the computed closure (update set).
      Conditional cond refs_a refs_b ->
            -- Compute the closure for the true-case.
        let (updset_a, hmaps_a) = foldl' (handle_ref vis) (updset0,hmaps0) refs_a
            -- Compute the closure for the false-case.
            (updset_b, hmaps_b) = foldl' (handle_ref vis) (updset0,hmaps_a) refs_b
            -- Move the conditional inwards, in the update set.
            -- TODO: This has the unfortunate side effect of moving all updates inside the conditional.
            updset1 = introCond $ createCond cond updset_a updset_b
        in (updset1, hmaps_b)
