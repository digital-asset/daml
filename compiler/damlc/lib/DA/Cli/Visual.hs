-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings   #-}
-- | Main entry-point of the DAML compiler
module DA.Cli.Visual
  ( execVisual
  ) where


import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.World as AST
import DA.Daml.LF.Reader
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified DA.Pretty as DAP
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified Codec.Archive.Zip as ZIPArchive
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as B
import Data.Generics.Uniplate.Data
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

type IsConsuming = Bool
type InternalChcName = LF.ChoiceName
data Action = ACreate (LF.Qualified LF.TypeConName)
            | AExercise (LF.Qualified LF.TypeConName) LF.ChoiceName deriving (Eq, Ord, Show )

data ChoiceAndAction = ChoiceAndAction
    { choiceName :: LF.ChoiceName
    , internalChcName :: InternalChcName -- as we have choices with same name across modules
    , choiceConsuming :: IsConsuming
    , actions :: Set.Set Action
    }

data TemplateChoices = TemplateChoices
    { template :: LF.Template
    , choiceAndActions :: [ChoiceAndAction]
    }

data ChoiceDetails = ChoiceDetails
    { nodeId :: Int
    , consuming :: Bool
    , displayChoiceName :: LF.ChoiceName
    }

data SubGraph = SubGraph
    { nodes :: [(ChoiceDetails, ChoiceDetails)]
    , clusterTemplate :: LF.Template
    }

startFromUpdate :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World -> LF.Update -> Set.Set Action
startFromUpdate seen world update = case update of
    LF.UPure _ e -> startFromExpr seen world e
    LF.UBind (LF.Binding _ e1) e2 -> startFromExpr seen world e1 `Set.union` startFromExpr seen world e2
    LF.UCreate tpl e -> Set.singleton (ACreate tpl) `Set.union` startFromExpr seen world e
    LF.UExercise tpl chc e1 e2 e3 -> Set.singleton (AExercise tpl chc) `Set.union` startFromExpr seen world e1 `Set.union` maybe Set.empty (startFromExpr seen world) e2 `Set.union` startFromExpr seen world e3
    LF.UFetch _ ctIdEx -> startFromExpr seen world ctIdEx
    LF.UGetTime -> Set.empty
    LF.UEmbedExpr _ upEx -> startFromExpr seen world upEx
    LF.ULookupByKey _ -> Set.empty
    LF.UFetchByKey _ -> Set.empty

startFromExpr :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World  -> LF.Expr -> Set.Set Action
startFromExpr seen world e = case e of
    LF.EVar _ -> Set.empty
    LF.EVal ref ->  case LF.lookupValue ref world of
        Right LF.DefValue{..}
            | ref `Set.member` seen  -> Set.empty
            | otherwise -> startFromExpr (Set.insert ref seen)  world dvalBody
        Left _ -> error "This should not happen"
    LF.EUpdate upd -> startFromUpdate seen world upd
    LF.ETmApp (LF.ETyApp (LF.EVal (LF.Qualified _ (LF.ModuleName ["DA","Internal","Template"]) (LF.ExprValName "fetch"))) _) _ -> Set.empty
    expr -> Set.unions $ map (startFromExpr seen world) $ children expr

startFromChoice :: LF.World -> LF.TemplateChoice -> Set.Set Action
startFromChoice world chc = startFromExpr Set.empty world (LF.chcUpdate chc)

templatePossibleUpdates :: LF.World -> LF.Template -> [ChoiceAndAction]
templatePossibleUpdates world tpl = actions
    where actions =  map (\c ->
            ChoiceAndAction
                (LF.chcName c)
                (LF.ChoiceName $ tplNameUnqual tpl <> (LF.unChoiceName .LF.chcName) c)
                (LF.chcConsuming c)
                (startFromChoice world c))
                (NM.toList (LF.tplChoices tpl))

moduleAndTemplates :: LF.World -> LF.Module -> [TemplateChoices]
moduleAndTemplates world mod = map (\t -> TemplateChoices t (templatePossibleUpdates world t)) $ NM.toList $ LF.moduleTemplates mod

dalfBytesToPakage :: BSL.ByteString -> ExternalPackage
dalfBytesToPakage bytes = case Archive.decodeArchive $ BSL.toStrict bytes of
    Right (pkgId, pkg) -> rewriteSelfReferences pkgId pkg
    Left err -> error (show err)

darToWorld :: ManifestData -> LF.Package -> LF.World
darToWorld manifest pkg = AST.initWorldSelf pkgs pkg
    where
        pkgs = map dalfBytesToPakage (dalfsContent manifest)

tplNameUnqual :: LF.Template -> T.Text
tplNameUnqual LF.Template {..} = head (LF.unTypeConName tplTypeCon)

extractChoiceData :: ChoiceAndAction -> (LF.ChoiceName, InternalChcName, IsConsuming)
extractChoiceData ChoiceAndAction {..} = (choiceName, internalChcName, choiceConsuming)

-- As create is never seen as a choice
templateWithCreateChoice :: TemplateChoices -> [(LF.ChoiceName, InternalChcName, IsConsuming)]
templateWithCreateChoice TemplateChoices {..} = createChoice : map extractChoiceData choiceAndActions
    where createChoice = (LF.ChoiceName "Create", LF.ChoiceName $ tplNameUnqual template <> "_Create", False)

-- Adding create as a choice to the graph
choiceNameWithId :: [TemplateChoices] -> Map.Map InternalChcName ChoiceDetails
choiceNameWithId tplChcActions = Map.fromList choiceWithIds
  where choiceActions = concatMap templateWithCreateChoice tplChcActions
        choiceWithIds = map (\((cName, inAlias, consume), id) -> (inAlias, ChoiceDetails id consume cName)) $ zip choiceActions [0..]

nodeIdForChoice :: Map.Map LF.ChoiceName ChoiceDetails -> LF.ChoiceName -> ChoiceDetails
nodeIdForChoice nodeLookUp chc = case Map.lookup chc nodeLookUp of
  Just node -> node
  Nothing -> error "Template node lookup failed"

addCreateChoice :: TemplateChoices -> Map.Map LF.ChoiceName ChoiceDetails -> (ChoiceDetails, ChoiceDetails)
addCreateChoice TemplateChoices {..} lookupData = (createChoiceDetails, createChoiceDetails)
    where tplNameCreateChoice = LF.ChoiceName $ T.pack $ DAP.renderPretty (head (LF.unTypeConName (LF.tplTypeCon template))) ++ "_Create"
          createChoiceDetails = nodeIdForChoice lookupData tplNameCreateChoice

constructSubgraphsWithLables :: Map.Map LF.ChoiceName ChoiceDetails -> TemplateChoices -> SubGraph
constructSubgraphsWithLables lookupData tpla@TemplateChoices {..} = SubGraph nodesWithCreate template
  where choicesInTemplete = map extractChoiceData choiceAndActions
        nodes = map (\(_, inChc, _) -> (nodeIdForChoice lookupData inChc, nodeIdForChoice lookupData inChc)) choicesInTemplete
        nodesWithCreate = nodes ++ [addCreateChoice tpla lookupData]

tplNamet :: LF.TypeConName -> T.Text
tplNamet tplConName = head (LF.unTypeConName tplConName)

actionToChoice :: Action -> LF.ChoiceName
actionToChoice (ACreate LF.Qualified {..}) = LF.ChoiceName $ tplNamet qualObject <> "_Create"
actionToChoice (AExercise LF.Qualified {..} (LF.ChoiceName chcT)) = LF.ChoiceName $ tplNamet qualObject <> chcT

choiceActionToChoicePairs :: ChoiceAndAction -> [(LF.ChoiceName, LF.ChoiceName)]
choiceActionToChoicePairs cha@ChoiceAndAction {..} = pairs
    where pairs = map (\ac -> (inCh, actionToChoice ac)) (Set.elems actions)
          (_, inCh, _) = extractChoiceData cha

graphEdges :: Map.Map LF.ChoiceName ChoiceDetails -> [TemplateChoices] -> [(ChoiceDetails, ChoiceDetails)]
graphEdges lookupData tplChcActions = map (\(chn1, chn2) -> (nodeIdForChoice lookupData chn1, nodeIdForChoice lookupData chn2)) choicePairsForTemplates
  where chcActionsFromAllTemplates = concatMap choiceAndActions tplChcActions
        choicePairsForTemplates = concatMap choiceActionToChoicePairs chcActionsFromAllTemplates

subGraphHeader :: LF.Template -> String
subGraphHeader tpl = "subgraph cluster_" ++ (DAP.renderPretty $ head (LF.unTypeConName $ LF.tplTypeCon tpl)) ++ "{\n"

choiceDetailsColorCode :: IsConsuming -> String
choiceDetailsColorCode True = "red"
choiceDetailsColorCode False = "green"

subGraphBodyLine :: (ChoiceDetails, ChoiceDetails) -> String
subGraphBodyLine (chcFrm, chcTo) = "n" ++ show (nodeId chcTo)++ "[label=" ++ DAP.renderPretty (displayChoiceName chcFrm) ++"][color=" ++ choiceDetailsColorCode (consuming chcTo) ++"]; "

subGraphEnd :: LF.Template -> String
subGraphEnd tpl = "label=" ++ DAP.renderPretty (LF.tplTypeCon tpl) ++ ";color=" ++ "blue" ++ "\n}"

subGraphCluster :: SubGraph -> String
subGraphCluster SubGraph {..} = subGraphHeader clusterTemplate ++ unlines (map subGraphBodyLine nodes) ++ subGraphEnd clusterTemplate

drawEdge :: ChoiceDetails -> ChoiceDetails -> String
drawEdge n1 n2 = "n" ++ show (nodeId n1) ++ "->" ++ "n" ++ show (nodeId n2)

constructDotGraph :: [SubGraph] -> [(ChoiceDetails, ChoiceDetails)] -> String
constructDotGraph subgraphs edges = "digraph G {\ncompound=true;\n" ++ "rankdir=LR;\n"++ graphLines ++ "\n}\n"
  where subgraphsLines = concatMap subGraphCluster subgraphs
        edgesLines = unlines $ map (uncurry drawEdge) edges
        graphLines = subgraphsLines ++ edgesLines

execVisual :: FilePath -> Maybe FilePath -> IO ()
execVisual darFilePath dotFilePath = do
    darBytes <- B.readFile darFilePath
    let manifestData = manifestFromDar $ ZIPArchive.toArchive (BSL.fromStrict darBytes)
    (_, lfPkg) <- errorOnLeft "Cannot decode package" $ Archive.decodeArchive (BSL.toStrict (mainDalfContent manifestData) )
    let modules = NM.toList $ LF.packageModules lfPkg
        world = darToWorld manifestData lfPkg
        templatesAndModules = concatMap (moduleAndTemplates world) modules
        nodeWorld = choiceNameWithId templatesAndModules
        subgraphClusters = map (constructSubgraphsWithLables nodeWorld) templatesAndModules
        graphConnectedEdges = graphEdges nodeWorld templatesAndModules
        dotString = constructDotGraph subgraphClusters graphConnectedEdges
    case dotFilePath of
        Just outDotFile -> writeFile outDotFile dotString
        Nothing -> putStrLn dotString

errorOnLeft :: Show a => String -> Either a b -> IO b
errorOnLeft desc = \case
  Left err -> ioError $ userError $ unlines [ desc, show err ]
  Right x  -> return x

