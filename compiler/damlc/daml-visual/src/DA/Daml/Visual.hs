-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE PatternSynonyms #-}
-- | Main entry-point of the DAML compiler
module DA.Daml.Visual
  ( execVisual
  , tplNameUnqual
  , TemplateChoices(..)
  , ChoiceAndAction(..)
  , Action(..)
  , Graph(..)
  , SubGraph(..)
  , ChoiceDetails(..)
  , dotFileGen
  , graphFromModule
  ) where


import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.World as AST
import DA.Daml.LF.Reader
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified DA.Pretty as DAP
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified "zip-archive" Codec.Archive.Zip as ZIPArchive
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as B
import Data.Generics.Uniplate.Data
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Safe

type IsConsuming = Bool
type InternalChcName = LF.ChoiceName

data Action = ACreate (LF.Qualified LF.TypeConName)
            | AExercise (LF.Qualified LF.TypeConName) LF.ChoiceName deriving (Eq, Ord, Show )

data ChoiceAndAction = ChoiceAndAction
    { choiceName :: LF.ChoiceName
    , internalChcName :: InternalChcName -- as we have choices with same name across modules
    , choiceConsuming :: IsConsuming
    , actions :: Set.Set Action
    } deriving (Show)

data TemplateChoices = TemplateChoices
    { template :: LF.Template
    , modName :: LF.ModuleName
    , choiceAndActions :: [ChoiceAndAction]
    } deriving (Show)

data ChoiceDetails = ChoiceDetails
    { nodeId :: Int
    , consuming :: Bool
    , displayChoiceName :: LF.ChoiceName
    } deriving (Show, Eq)

data SubGraph = SubGraph
    { nodes :: [ChoiceDetails]
    , templateFields :: [T.Text]
    , clusterTemplate :: LF.Template
    } deriving (Show, Eq)

data Graph = Graph
    { subgraphs :: [SubGraph]
    , edges :: [(ChoiceDetails, ChoiceDetails)]
    } deriving (Show, Eq)

startFromUpdate :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World -> LF.Update -> Set.Set Action
startFromUpdate seen world update = case update of
    LF.UPure _ e -> startFromExpr seen world e
    LF.UBind (LF.Binding _ e1) e2 -> startFromExpr seen world e1 `Set.union` startFromExpr seen world e2
    LF.UGetTime -> Set.empty
    LF.UEmbedExpr _ upEx -> startFromExpr seen world upEx
    -- NOTE(MH): The cases below are impossible because they only appear
    -- in dictionaries for the `Template` and `Choice` classes, which we
    -- ignore below.
    LF.UCreate{} -> error "IMPOSSIBLE"
    LF.UExercise{} -> error "IMPOSSIBLE"
    LF.UFetch{} -> error "IMPOSSIBLE"
    LF.ULookupByKey{} -> error "IMPOSSIBLE"
    LF.UFetchByKey{} -> error "IMPOSSIBLE"

startFromExpr :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World -> LF.Expr -> Set.Set Action
startFromExpr seen world e = case e of
    LF.EVar _ -> Set.empty
    -- NOTE(MH/RJR): Do not explore the `$fXInstance` dictionary because it
    -- contains all the ledger actions and therefore creates too many edges
    -- in the graph. We instead detect calls to the `create`, `archive` and
    -- `exercise` methods from `Template` and `Choice` instances.
    LF.EVal (LF.Qualified _ _ (LF.ExprValName ref))
        | "$f" `T.isPrefixOf` ref && "Instance" `T.isSuffixOf` ref -> Set.empty
    LF.EVal ref -> case LF.lookupValue ref world of
        Right LF.DefValue{..}
            | ref `Set.member` seen -> Set.empty
            | otherwise -> startFromExpr (Set.insert ref seen) world dvalBody
        Left _ -> error "This should not happen"
    LF.EUpdate upd -> startFromUpdate seen world upd
    -- NOTE(RJR): Look for calls to `create` and `archive` methods from a
    -- `Template` instance and produce the corresponding edges in the graph.
    EInternalTemplateVal "create" `LF.ETyApp` LF.TCon tpl `LF.ETmApp` _dict
        -> Set.singleton (ACreate tpl)
    EInternalTemplateVal "archive" `LF.ETyApp` LF.TCon tpl `LF.ETmApp` _dict ->
        Set.singleton (AExercise tpl (LF.ChoiceName "Archive"))
    -- NOTE(RJR): Look for calls to the `exercise` method from a `Choice`
    -- instance and produce the corresponding edge in the graph.
    EInternalTemplateVal "exercise" `LF.ETyApp` LF.TCon tpl `LF.ETyApp` LF.TCon (LF.Qualified _ _ (LF.TypeConName [chc])) `LF.ETyApp` _ret `LF.ETmApp` _dict ->
        Set.singleton (AExercise tpl (LF.ChoiceName chc))
    expr -> Set.unions $ map (startFromExpr seen world) $ children expr

pattern EInternalTemplateVal :: T.Text -> LF.Expr
pattern EInternalTemplateVal val <-
    LF.EVal (LF.Qualified _pkg (LF.ModuleName ["DA", "Internal", "Template"]) (LF.ExprValName val))

startFromChoice :: LF.World -> LF.TemplateChoice -> Set.Set Action
startFromChoice world chc = startFromExpr Set.empty world (LF.chcUpdate chc)

templatePossibleUpdates :: LF.World -> LF.Template -> [ChoiceAndAction]
templatePossibleUpdates world tpl = map toActions $ NM.toList $ LF.tplChoices tpl
    where toActions c = ChoiceAndAction {
                choiceName = LF.chcName c
              , internalChcName = LF.ChoiceName $ tplNameUnqual tpl <> (LF.unChoiceName .LF.chcName) c
              , choiceConsuming = LF.chcConsuming c
              , actions = startFromChoice world c
              }

moduleAndTemplates :: LF.World -> LF.Module -> [TemplateChoices]
moduleAndTemplates world mod = map (\t -> TemplateChoices t (LF.moduleName mod) (templatePossibleUpdates world t)) $ NM.toList $ LF.moduleTemplates mod

dalfBytesToPakage :: BSL.ByteString -> ExternalPackage
dalfBytesToPakage bytes = case Archive.decodeArchive $ BSL.toStrict bytes of
    Right (pkgId, pkg) -> rewriteSelfReferences pkgId pkg
    Left err -> error (show err)

darToWorld :: Dalfs -> LF.World
darToWorld Dalfs{..} = case Archive.decodeArchive $ BSL.toStrict mainDalf of
    Right (_, mainPkg) -> AST.initWorldSelf pkgs mainPkg
    Left err -> error (show err)
    where
        pkgs = map dalfBytesToPakage dalfs

tplNameUnqual :: LF.Template -> T.Text
tplNameUnqual LF.Template {..} = headNote "tplNameUnqual" (LF.unTypeConName tplTypeCon)

choiceNameWithId :: [TemplateChoices] -> Map.Map InternalChcName ChoiceDetails
choiceNameWithId tplChcActions = Map.fromList choiceWithIds
  where choiceWithIds = map (\(ChoiceAndAction {..}, id) -> (internalChcName, ChoiceDetails id choiceConsuming choiceName)) $ zip choiceActions [0..]
        choiceActions = concatMap (\t -> createChoice (template t) : choiceAndActions t) tplChcActions
        createChoice tpl = ChoiceAndAction
            { choiceName = LF.ChoiceName "Create"
            , internalChcName = LF.ChoiceName $ tplNameUnqual tpl <> "_Create"
            , choiceConsuming = False
            , actions = Set.empty
            }

nodeIdForChoice :: Map.Map LF.ChoiceName ChoiceDetails -> LF.ChoiceName -> ChoiceDetails
nodeIdForChoice nodeLookUp chc = case Map.lookup chc nodeLookUp of
  Just node -> node
  Nothing -> error "Template node lookup failed"

addCreateChoice :: TemplateChoices -> Map.Map LF.ChoiceName ChoiceDetails -> ChoiceDetails
addCreateChoice TemplateChoices {..} lookupData = nodeIdForChoice lookupData tplNameCreateChoice
    where tplNameCreateChoice = LF.ChoiceName $ T.pack $ DAP.renderPretty (headNote "addCreateChoice" (LF.unTypeConName (LF.tplTypeCon template))) ++ "_Create"

labledField :: T.Text -> T.Text -> T.Text
labledField fname "" = fname
labledField fname label = fname <> "." <> label

typeConFieldsNames :: LF.World -> (LF.FieldName, LF.Type) -> [T.Text]
typeConFieldsNames world (LF.FieldName fName, LF.TConApp tcn _) = map (labledField fName) (typeConFields tcn world)
typeConFieldsNames _ (LF.FieldName fName, _) = [fName]

-- TODO: Anup This will fail if we were to recursively continue exploring the AST.
typeConFields :: LF.Qualified LF.TypeConName -> LF.World -> [T.Text]
typeConFields qName world = case LF.lookupDataType qName world of
  Right dataType -> case LF.dataCons dataType of
    LF.DataRecord re -> concatMap (typeConFieldsNames world) re
    LF.DataVariant _ -> [""]
    LF.DataEnum _ -> [""]
  Left _ -> error "malformed template constructor"

constructSubgraphsWithLables :: LF.World -> Map.Map LF.ChoiceName ChoiceDetails -> TemplateChoices -> SubGraph
constructSubgraphsWithLables wrld lookupData tpla@TemplateChoices {..} = SubGraph nodesWithCreate fieldsInTemplate template
  where choicesInTemplate = map internalChcName choiceAndActions
        fieldsInTemplate = typeConFields  qualTpl wrld
        nodes = map (nodeIdForChoice lookupData) choicesInTemplate
        qualTpl = LF.Qualified LF.PRSelf modName (LF.tplTypeCon template)
        nodesWithCreate = addCreateChoice tpla lookupData : nodes

tplNamet :: LF.TypeConName -> T.Text
tplNamet tplConName = headNote "tplNamet" (LF.unTypeConName tplConName)

actionToChoice :: Action -> LF.ChoiceName
actionToChoice (ACreate LF.Qualified {..}) = LF.ChoiceName $ tplNamet qualObject <> "_Create"
actionToChoice (AExercise LF.Qualified {..} (LF.ChoiceName chcT)) = LF.ChoiceName $ tplNamet qualObject <> chcT

choiceActionToChoicePairs :: ChoiceAndAction -> [(LF.ChoiceName, LF.ChoiceName)]
choiceActionToChoicePairs ChoiceAndAction{..} = pairs
    where pairs = map (\ac -> (internalChcName, actionToChoice ac)) (Set.elems actions)

graphEdges :: Map.Map LF.ChoiceName ChoiceDetails -> [TemplateChoices] -> [(ChoiceDetails, ChoiceDetails)]
graphEdges lookupData tplChcActions = map (\(chn1, chn2) -> (nodeIdForChoice lookupData chn1, nodeIdForChoice lookupData chn2)) choicePairsForTemplates
  where chcActionsFromAllTemplates = concatMap choiceAndActions tplChcActions
        choicePairsForTemplates = concatMap choiceActionToChoicePairs chcActionsFromAllTemplates

subGraphHeader :: SubGraph -> String
subGraphHeader sg = "subgraph cluster_" ++ (DAP.renderPretty $ head (LF.unTypeConName $ LF.tplTypeCon $ clusterTemplate sg)) ++ "{\n"

choiceDetailsColorCode :: IsConsuming -> String
choiceDetailsColorCode True = "red"
choiceDetailsColorCode False = "green"

subGraphBodyLine :: ChoiceDetails -> String
subGraphBodyLine chc = "n" ++ show (nodeId chc)++ "[label=" ++ DAP.renderPretty (displayChoiceName chc) ++"][color=" ++ choiceDetailsColorCode (consuming chc) ++"]; "

subGraphEnd :: SubGraph -> String
subGraphEnd sg = "label=<" ++ tHeader ++ tTitle ++ tBody  ++ tclose ++ ">" ++ ";color=" ++ "blue" ++ "\n}"
    where tHeader = "<table align = \"left\" border=\"0\" cellborder=\"0\" cellspacing=\"1\">\n"
          tTitle =  "<tr><td align=\"center\"><b>" ++  DAP.renderPretty (LF.tplTypeCon $ clusterTemplate sg) ++ "</b></td></tr>"
          tBody = concatMap fieldTableLine (templateFields sg)
          fieldTableLine field = "<tr><td align=\"left\">" ++ T.unpack field  ++ "</td></tr> \n"
          tclose = "</table>"

subGraphCluster :: SubGraph -> String
subGraphCluster sg@SubGraph {..} = subGraphHeader sg ++ unlines (map subGraphBodyLine nodes) ++ subGraphEnd sg

drawEdge :: ChoiceDetails -> ChoiceDetails -> String
drawEdge n1 n2 = "n" ++ show (nodeId n1) ++ "->" ++ "n" ++ show (nodeId n2)

constructDotGraph :: Graph -> String
constructDotGraph graph  = "digraph G {\ncompound=true;\n" ++ "rankdir=LR;\n"++ graphLines ++ "\n}\n"
  where subgraphsLines = concatMap subGraphCluster (subgraphs graph)
        edgesLines = unlines $ map (uncurry drawEdge) (edges graph)
        graphLines = subgraphsLines ++ edgesLines

graphFromModule :: [LF.Module] -> LF.World -> Graph
graphFromModule modules world = Graph subGraphs edges
    where templatesAndModules = concatMap (moduleAndTemplates world) modules
          nodes = choiceNameWithId templatesAndModules
          subGraphs = map (constructSubgraphsWithLables world nodes) templatesAndModules
          edges = graphEdges nodes templatesAndModules


dotFileGen :: [LF.Module] -> LF.World -> String
dotFileGen modules world = constructDotGraph $ graphFromModule modules world

execVisual :: FilePath -> Maybe FilePath -> IO ()
execVisual darFilePath dotFilePath = do
    darBytes <- B.readFile darFilePath
    dalfs <- either fail pure $ readDalfs $ ZIPArchive.toArchive (BSL.fromStrict darBytes)
    let world = darToWorld dalfs
        modules = NM.toList $ LF.packageModules $ getWorldSelf world
    case dotFilePath of
        Just outDotFile -> writeFile outDotFile (dotFileGen modules world)
        Nothing -> putStrLn (dotFileGen modules world)
