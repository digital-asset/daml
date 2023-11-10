-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.PrettyScenarioSpec (main) where

import DA.Daml.LF.PrettyScenario
import ScenarioService qualified as S

import Control.Monad.State.Strict
import Data.Bifunctor
import Data.Text.Lazy qualified as TL
import Data.Vector qualified as V
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = defaultMain $ testGroup "PrettyScenario"
  [ ptxExerciseContextTests
  ]

ctx :: String -> ExerciseContext
ctx choice = ExerciseContext
  { targetId = Just (S.ContractRef "#0" Nothing)
  , choiceId = TL.pack choice
  , exerciseLocation = Nothing
  , chosenValue = Nothing
  , exerciseKey = Nothing
  }

ptxExerciseContextTests :: TestTree
ptxExerciseContextTests = testGroup "ptxExerciseContext"
  [ testCase "returns the last root exercise node" $ do
      ptxExerciseContext (toPtx [Exercise "1" False [], Exercise "2" False []]) @?= Just (ctx "2")
  , testCase "ignores complete exercise nodes" $ do
      ptxExerciseContext (toPtx [Exercise "1" False [], Exercise "2" True []]) @?= Nothing
  , testCase "ignores create, fetch and lookup nodes" $ do
      ptxExerciseContext (toPtx [Create, Fetch, Lookup]) @?= Nothing
  , testCase "does not decend in rollback node" $ do
      ptxExerciseContext (toPtx [Rollback [Exercise "1" False []]]) @?= Nothing
  , testCase "decends in exercise" $ do
      ptxExerciseContext (toPtx [Exercise "0" False [Exercise "1" False []]]) @?= Just (ctx "1")
  ]

type Transaction = [Node]

data Node
    = Create
    | Lookup
    | Fetch
    | Exercise String Bool [Node]
    | Rollback [Node]

toPtx :: Transaction -> S.PartialTransaction
toPtx nodes = case runState (mapM go nodes) (0, []) of
    (roots, (_, nodes)) -> S.PartialTransaction
      { partialTransactionRoots = V.fromList roots
      , partialTransactionNodes = V.fromList (reverse nodes)
      }
  where
      go :: Node -> State (Int, [S.Node]) S.NodeId
      go n = do
          nid <- nextNodeId
          nodeNode <- case n of
              Create -> pure $ S.NodeNodeCreate S.Node_Create
                { node_CreateContractInstance = Nothing
                , node_CreateSignatories = V.empty
                , node_CreateStakeholders = V.empty
                , node_CreateKeyWithMaintainers = Nothing
                , node_CreateContractId = "#0"
                }
              Fetch -> pure $ S.NodeNodeFetch S.Node_Fetch
                { node_FetchContractId = "#0"
                , node_FetchTemplateId = Nothing
                , node_FetchActingParties = V.empty
                , node_FetchFetchByKey = Nothing
                }
              Lookup -> pure $ S.NodeNodeLookupByKey S.Node_LookupByKey
                { node_LookupByKeyTemplateId = Nothing
                , node_LookupByKeyKeyWithMaintainers = Nothing
                , node_LookupByKeyContractId = "#0"
                }
              Rollback children -> do
                  children' <- mapM go children
                  pure $ S.NodeNodeRollback (S.Node_Rollback (V.fromList children'))
              Exercise choice complete children -> do
                  children' <- mapM go children
                  pure $ S.NodeNodeExercise S.Node_Exercise
                    { node_ExerciseTargetContractId = "#0"
                    , node_ExerciseTemplateId = Nothing
                    , node_ExerciseChoiceId = TL.pack choice
                    , node_ExerciseActingParties = V.empty
                    , node_ExerciseChosenValue = Nothing
                    , node_ExerciseObservers = V.empty
                    , node_ExerciseSignatories = V.empty
                    , node_ExerciseStakeholders = V.empty
                    , node_ExerciseChildren = V.fromList children'
                    , node_ExerciseExerciseResult = if complete then Just (S.Value (Just (S.ValueSumUnit S.Empty))) else Nothing
                    , node_ExerciseConsuming = False
                    , node_ExerciseExerciseByKey = Nothing
                    }
          let node = S.Node
                { nodeNodeId = Just nid
                , nodeNode = Just nodeNode
                , nodeEffectiveAt = 0
                , nodeDisclosures = V.empty
                , nodeReferencedBy = V.empty
                , nodeConsumedBy = Nothing
                , nodeRolledbackBy = Nothing
                , nodeParent = Nothing
                , nodeLocation = Nothing
                }
          modify' $ second (node :)
          pure nid
      nextNodeId :: State (Int, b) S.NodeId
      nextNodeId = do
          (i, nodes) <- get
          let !i' = i + 1
          put (i', nodes)
          pure (S.NodeId (TL.pack $ show i))

