-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.Core.API.Testing.Visualize
    ( ExpectedGraph(..)
    , ExpectedSubGraph(..)
    , ExpectedChoiceDetails(..)
    , FailedGraphExpectation(..)
    , graphTest
    )
    where

import Control.Monad
import Data.Bifunctor
import qualified Data.Text as T

import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.Visual as V

type TemplateName = String
type ChoiceName = String

data ExpectedGraph = ExpectedGraph
    { expectedSubgraphs :: [ExpectedSubGraph]
    , expectedEdges :: [(ExpectedChoiceDetails, ExpectedChoiceDetails)]
    } deriving (Eq, Ord, Show )

data ExpectedSubGraph = ExpectedSubGraph
    { expectedNodes :: [ChoiceName]
    , expectedTplFields :: [String]
    , expectedTemplate :: TemplateName
    } deriving (Eq, Ord, Show )

data ExpectedChoiceDetails = ExpectedChoiceDetails
    { expectedConsuming :: Bool
    , expectedName :: String
    } deriving (Eq, Ord, Show )

subgraphToExpectedSubgraph :: V.SubGraph -> ExpectedSubGraph
subgraphToExpectedSubgraph vSubgraph = ExpectedSubGraph vNodes vFields vTplName
    where vNodes = map (T.unpack . LF.unChoiceName . V.displayChoiceName) (V.nodes vSubgraph)
          vFields = map T.unpack (V.templateFields vSubgraph)
          vTplName = T.unpack $ V.tplNameUnqual (V.clusterTemplate vSubgraph)

graphToExpectedGraph :: V.Graph -> ExpectedGraph
graphToExpectedGraph vGraph = ExpectedGraph vSubgrpaghs vEdges
    where vSubgrpaghs = map subgraphToExpectedSubgraph (V.subgraphs vGraph)
          vEdges = map (bimap expectedChcDetails expectedChcDetails) (V.edges vGraph)
          expectedChcDetails chc = ExpectedChoiceDetails (V.consuming chc)
                                ((T.unpack . LF.unChoiceName . V.displayChoiceName) chc)


data FailedGraphExpectation = FailedGraphExpectation
  { expected :: ExpectedGraph
  , actual :: ExpectedGraph
  }
  deriving (Eq, Show)

graphTest :: LF.World -> ExpectedGraph -> Either FailedGraphExpectation ()
graphTest wrld expectedGraph = do
    let actualGraph = V.graphFromWorld wrld
    let actual = graphToExpectedGraph actualGraph
    unless (expectedGraph == actual) $
        Left $ FailedGraphExpectation expectedGraph actual
