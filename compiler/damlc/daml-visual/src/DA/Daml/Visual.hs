-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE PatternSynonyms #-}
-- | Main entry-point of the Daml compiler
module DA.Daml.Visual
  ( execVisual
  , nameUnqual
  , Choices(..)
  , ChoiceWithActions(..)
  , Action(..)
  , Graph(..)
  , SubGraph(..)
  , ChoiceDetails(..)
  , dotFileGen
  , graphFromWorld
  , execVisualHtml
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
import Data.Tuple.Extra (both)
import GHC.Generics
import Data.Aeson
import Text.Mustache
import qualified Data.Text.Lazy.IO as TIO
import qualified Data.Text.Encoding as DT
import Web.Browser
import DA.Bazel.Runfiles
import System.FilePath
import Safe
import Control.Monad
import Control.Monad.State

type IsConsuming = Bool

data Action = ACreate (LF.Qualified LF.TypeConName)
            | AExercise (LF.Qualified LF.TypeConName) LF.ChoiceName deriving (Eq, Ord, Show )

data ChoiceWithActions = ChoiceWithActions
    { choiceName :: LF.ChoiceName
    , choiceConsuming :: IsConsuming
    , actions :: Set.Set Action
    } deriving (Show)


data Choices
  = Choices
    { templateOrInterface :: LF.Qualified (Either LF.Template LF.DefInterface)
    , choiceWithActions :: [ChoiceWithActions]
    }
    deriving (Show)

choicesParentId :: Choices -> LF.Qualified LF.TypeConName
choicesParentId Choices{..} =
    fmap (either LF.tplTypeCon LF.intName) templateOrInterface

data ChoiceDetails = ChoiceDetails
    { nodeId :: Int
    , consuming :: Bool
    , displayChoiceName :: LF.ChoiceName
    } deriving (Show, Eq)

data SubGraph = SubGraph
    { nodes :: [ChoiceDetails]
    , templateFields :: [T.Text]
    , clusterName :: LF.TypeConName
    } deriving (Show, Eq)

data Graph = Graph
    { subgraphs :: [SubGraph]
    , edges :: [(ChoiceDetails, ChoiceDetails)]
    , subgraphEdges :: [(LF.TypeConName, LF.TypeConName)]
    } deriving (Show, Eq)

data D3Link = D3Link
    { source :: Int
    , target :: Int
    , weight :: Int
    } deriving (Generic, Show)

-- can add more information like signatories, observers
data D3Node = D3Node
    { fields :: T.Text
    , tplName :: T.Text
    , id :: Int
    , chcName :: T.Text
    } deriving (Generic, Show)

data D3Graph = D3Graph
    { d3links :: [D3Link]
    , d3nodes :: [D3Node]
    } deriving (Generic, Show)

data WebPage = WebPage
    { links :: T.Text
    , dnodes :: T.Text
    , d3Js :: String
    , d3PlusJs :: String
    } deriving (Generic, Show)

d3LinksFromGraphEdges :: Graph -> [D3Link]
d3LinksFromGraphEdges g = map edgeToD3Link (edges g)
    where edgeToD3Link edge = D3Link (nodeId (fst edge)) (nodeId (snd edge)) 10

d3NodesFromGraph :: Graph -> [D3Node]
d3NodesFromGraph g = concatMap subGraphToD3Nodes (subgraphs g)
        where subGraphToD3Nodes sg = map (\chcD ->
                                            D3Node (T.unlines $ templateFields sg)
                                            (nameUnqual $ clusterName sg)
                                            (nodeId chcD)
                                            (DAP.renderPretty $ displayChoiceName chcD)
                                            )
                                    (nodes sg)

graphToD3Graph :: Graph -> D3Graph
graphToD3Graph g = D3Graph (d3LinksFromGraphEdges g) (d3NodesFromGraph g)

instance ToJSON D3Link
instance ToJSON D3Node
instance ToJSON D3Graph
instance ToJSON WebPage

startFromUpdate :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World -> LF.Update -> Set.Set Action
startFromUpdate seen world update = case update of
    LF.UPure _ e -> startFromExpr seen world e
    LF.UBind (LF.Binding _ e1) e2 -> startFromExpr seen world e1 `Set.union` startFromExpr seen world e2
    LF.UGetTime -> Set.empty
    LF.UEmbedExpr _ upEx -> startFromExpr seen world upEx
    LF.UCreate tpl _ -> Set.singleton (ACreate tpl)
    LF.UCreateInterface iface _ -> Set.singleton (ACreate iface)
    LF.UExercise tpl choice _ _ -> Set.singleton (AExercise tpl choice)
    LF.UExerciseInterface iface choice _ _ _ -> Set.singleton (AExercise iface choice)
    LF.UExerciseByKey tpl choice _ _ -> Set.singleton (AExercise tpl choice)
    LF.UFetch{} -> Set.empty
    LF.UFetchInterface{} -> Set.empty
    LF.ULookupByKey{} -> Set.empty
    LF.UFetchByKey{} -> Set.empty
    LF.UTryCatch _ e1 _ e2 -> startFromExpr seen world e1 `Set.union` startFromExpr seen world e2

startFromExpr :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World -> LF.Expr -> Set.Set Action
startFromExpr seen world e = case e of
    LF.EVar _ -> Set.empty
    -- NOTE(MH/RJR): Do not explore the `$fChoice`/`$fTemplate` dictionaries because
    -- they contain all the ledger actions and therefore creates too many edges
    -- in the graph. We instead detect calls to the `create`, `archive` and
    -- `exercise` methods from `Template` and `Choice` instances.
    LF.EVal (LF.Qualified _ _ (LF.ExprValName ref))
        | any (`T.isPrefixOf` ref)
            [ "$fHasCreate"
            , "$fHasExercise"
            , "$fHasExerciseGuarded"
            , "$fHasArchive"
            , "$fHasFetch" -- also filters out $fHasFetchByKey
            , "$fHasLookupByKey"
            ] -> Set.empty
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
    EInternalTemplateVal "exerciseGuarded" `LF.ETyApp` LF.TCon iface `LF.ETyApp` LF.TCon (LF.Qualified _ _ (LF.TypeConName [chc])) `LF.ETyApp` _ret `LF.ETmApp` _dict ->
        Set.singleton (AExercise iface (LF.ChoiceName chc))
    EInternalTemplateVal "exerciseByKey" `LF.ETyApp` LF.TCon tpl `LF.ETyApp` _ `LF.ETyApp` LF.TCon (LF.Qualified _ _ (LF.TypeConName [chc])) `LF.ETyApp` _ret `LF.ETmApp` _dict ->
        Set.singleton (AExercise tpl (LF.ChoiceName chc))
    expr -> Set.unions $ map (startFromExpr seen world) $ children expr

pattern EInternalTemplateVal :: T.Text -> LF.Expr
pattern EInternalTemplateVal val <-
    LF.EVal (LF.Qualified _pkg (LF.ModuleName ["DA", "Internal", "Template", "Functions"]) (LF.ExprValName val))

startFromChoice :: LF.World -> LF.TemplateChoice -> Set.Set Action
startFromChoice world chc = startFromExpr Set.empty world (LF.chcUpdate chc)

templatePossibleUpdates :: LF.World -> LF.Template -> [ChoiceWithActions]
templatePossibleUpdates world tpl = map toActions $ NM.toList $ LF.tplChoices tpl
    where toActions c = ChoiceWithActions {
                choiceName = LF.chcName c
              , choiceConsuming = LF.chcConsuming c
              , actions = startFromChoice world c
              }

interfacePossibleUpdates :: LF.World -> LF.DefInterface -> [ChoiceWithActions]
interfacePossibleUpdates world iface = map toActions $ NM.toList $ LF.intChoices iface
    where toActions c = ChoiceWithActions {
                choiceName = LF.chcName c
              , choiceConsuming = LF.chcConsuming c
              , actions = startFromChoice world c
              }

moduleChoices :: LF.World -> LF.PackageRef -> LF.Module -> [Choices]
moduleChoices world pkgRef mod =
    let mkTemplateChoices t =
            Choices
                (LF.Qualified pkgRef (LF.moduleName mod) (Left t))
                (templatePossibleUpdates world t)
        mkInterfaceChoices i =
            Choices
                (LF.Qualified pkgRef (LF.moduleName mod) (Right i))
                (interfacePossibleUpdates world i)
    in
    map mkTemplateChoices (NM.toList $ LF.moduleTemplates mod) ++
    map mkInterfaceChoices (NM.toList $ LF.moduleInterfaces mod)

dalfBytesToPakage :: BSL.ByteString -> ExternalPackage
dalfBytesToPakage bytes = case Archive.decodeArchive Archive.DecodeAsDependency $ BSL.toStrict bytes of
    Right (pkgId, pkg) -> ExternalPackage pkgId pkg
    Left err -> error (show err)

darToWorld :: Dalfs -> LF.World
darToWorld Dalfs{..} = case Archive.decodeArchive Archive.DecodeAsMain $ BSL.toStrict mainDalf of
    Right (_, mainPkg) -> AST.initWorldSelf pkgs mainPkg
    Left err -> error (show err)
    where
        pkgs = map dalfBytesToPakage dalfs

nameUnqual :: LF.TypeConName -> T.Text
nameUnqual = headNote "nameUnqual" . LF.unTypeConName

data ChoiceIdentifier = ChoiceIdentifier
  { choiceIdTemplate :: !(LF.Qualified LF.TypeConName)
  , choiceIdName :: !LF.ChoiceName
  } deriving (Eq, Show, Ord)

choiceNameWithId :: [Choices] -> Map.Map ChoiceIdentifier ChoiceDetails
choiceNameWithId tplChcActions = Map.unions (evalState (mapM f tplChcActions) 0)
  where
    f :: Choices -> State Int (Map.Map ChoiceIdentifier ChoiceDetails)
    f tpl@Choices{..} = do
        choices <- forM (createChoice : choiceWithActions) $ \ChoiceWithActions{..} -> do
          id <- get
          put (id + 1)
          let choiceId = ChoiceIdentifier (choicesParentId tpl) choiceName
          pure (choiceId, ChoiceDetails id choiceConsuming choiceName)
        pure (Map.fromList choices)
    createChoice = ChoiceWithActions
        { choiceName = LF.ChoiceName "Create"
        , choiceConsuming = False
        , actions = Set.empty
        }

nodeIdForChoice :: Map.Map ChoiceIdentifier ChoiceDetails -> ChoiceIdentifier -> ChoiceDetails
nodeIdForChoice nodeLookUp chc = case Map.lookup chc nodeLookUp of
  Just node -> node
  Nothing -> error "Template node lookup failed"

addCreateChoice :: LF.Qualified LF.TypeConName -> Map.Map ChoiceIdentifier ChoiceDetails -> ChoiceDetails
addCreateChoice name lookupData = nodeIdForChoice lookupData tplNameCreateChoice
  where
    tplNameCreateChoice =
        ChoiceIdentifier
            name
            createChoiceName

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
    -- TODO https://github.com/digital-asset/daml/issues/12051
    LF.DataInterface -> [""]
  Left _ -> error "malformed template constructor"

constructSubgraphsWithLables :: LF.World -> Map.Map ChoiceIdentifier ChoiceDetails -> Choices -> SubGraph
constructSubgraphsWithLables wrld lookupData choices@Choices {..} =
    case LF.qualObject templateOrInterface of
      Left _ ->
        SubGraph (addCreateChoice parentId lookupData : choiceNodeIds) fields (LF.qualObject parentId)
      Right _ ->
        SubGraph choiceNodeIds fields (LF.qualObject parentId)
  where
    parentId = choicesParentId choices
    fields = typeConFields parentId wrld
    choiceNodeIds =
        map (\c -> nodeIdForChoice lookupData (ChoiceIdentifier parentId (choiceName c)))
            choiceWithActions

createChoiceName :: LF.ChoiceName
createChoiceName = LF.ChoiceName "Create"

actionToChoice :: Action -> ChoiceIdentifier
actionToChoice (ACreate tpl) =
    ChoiceIdentifier tpl createChoiceName
actionToChoice (AExercise tpl chcT) =
    ChoiceIdentifier tpl chcT

choiceActionToChoicePairs :: LF.Qualified LF.TypeConName -> ChoiceWithActions -> [(ChoiceIdentifier, ChoiceIdentifier)]
choiceActionToChoicePairs tpl ChoiceWithActions{..} =
    map (\a -> (choiceId, actionToChoice a)) (Set.elems actions)
  where
     choiceId = ChoiceIdentifier tpl choiceName

graphEdges :: Map.Map ChoiceIdentifier ChoiceDetails -> [Choices] -> [(ChoiceDetails, ChoiceDetails)]
graphEdges lookupData tplChcActions =
    map (both (nodeIdForChoice lookupData)) $
    concat $
    concatMap
        (\tpl -> map (choiceActionToChoicePairs (choicesParentId tpl)) (choiceWithActions tpl))
        tplChcActions

typeConNodeName :: LF.TypeConName -> String
typeConNodeName = DAP.renderPretty . head . LF.unTypeConName

subGraphHeader :: SubGraph -> String
subGraphHeader sg = "subgraph cluster_" ++ typeConNodeName (clusterName sg) ++ "{\nDUMMY_" ++ typeConNodeName (clusterName sg) ++ " [shape=point style=invis];\n"

choiceDetailsColorCode :: IsConsuming -> String
choiceDetailsColorCode True = "red"
choiceDetailsColorCode False = "green"

subGraphBodyLine :: ChoiceDetails -> String
subGraphBodyLine chc = "n" ++ show (nodeId chc)++ "[label=" ++ DAP.renderPretty (displayChoiceName chc) ++"][color=" ++ choiceDetailsColorCode (consuming chc) ++"]; "

subGraphEnd :: SubGraph -> String
subGraphEnd sg = "label=<" ++ tHeader ++ tTitle ++ tBody  ++ tclose ++ ">" ++ ";color=" ++ "blue" ++ "\n}"
    where tHeader = "<table align = \"left\" border=\"0\" cellborder=\"0\" cellspacing=\"1\">\n"
          tTitle =  "<tr><td align=\"center\"><b>" ++  DAP.renderPretty (clusterName sg) ++ "</b></td></tr>"
          tBody = concatMap fieldTableLine (templateFields sg)
          fieldTableLine field = "<tr><td align=\"left\">" ++ T.unpack field  ++ "</td></tr> \n"
          tclose = "</table>"

subGraphCluster :: SubGraph -> String
subGraphCluster sg@SubGraph {..} = subGraphHeader sg ++ unlines (map subGraphBodyLine nodes) ++ subGraphEnd sg

drawEdge :: ChoiceDetails -> ChoiceDetails -> String
drawEdge n1 n2 = "n" ++ show (nodeId n1) ++ "->" ++ "n" ++ show (nodeId n2)

-- TODO: Is there a better way of connecting two subgraphs via dummy nodes?
drawSubgraphEdge :: LF.TypeConName -> LF.TypeConName -> String
drawSubgraphEdge iface tpl =
    "DUMMY_" ++ typeConNodeName tpl ++ " -> DUMMY_" ++ typeConNodeName iface ++
        " [ltail=cluster_" ++ typeConNodeName tpl ++ ", lhead=cluster_" ++ typeConNodeName iface ++ "]"

constructDotGraph :: Graph -> String
constructDotGraph graph  = "digraph G {\ncompound=true;\n" ++ "rankdir=LR;\n"++ unlines graphLines ++ "\n}\n"
  where subgraphsLines = map subGraphCluster (subgraphs graph)
        edgesLines = map (uncurry drawEdge) (edges graph)
        subgraphEdgesLines = map (uncurry drawSubgraphEdge) (subgraphEdges graph)
        graphLines = subgraphsLines ++ edgesLines ++ subgraphEdgesLines

graphFromWorld :: LF.World -> Graph
graphFromWorld world = Graph subGraphs edges subgraphEdges
  where
    allChoices = concat
        [ moduleChoices world pkgRef mod
        | (pkgRef, pkg) <- pkgs
        , mod <- NM.toList $ LF.packageModules pkg
        ]
    nodes = choiceNameWithId allChoices
    subGraphs = map (constructSubgraphsWithLables world nodes) allChoices
    subgraphEdges = do
        choice <- allChoices
        case LF.qualObject (templateOrInterface choice) of
            Left template -> do
                implements <- NM.elems (LF.tplImplements template)
                pure (LF.qualObject (LF.tpiInterface implements), LF.tplTypeCon template)
            Right interface -> do
                coimplements <- NM.elems (LF.intCoImplements interface)
                pure (LF.intName interface, LF.qualObject (LF.iciTemplate coimplements))
    edges = graphEdges nodes allChoices
    pkgs =
        (LF.PRSelf, getWorldSelf world)
        : map (\ExternalPackage{..} -> (LF.PRImport extPackageId, extPackagePkg))
              (getWorldImported world)

dotFileGen :: LF.World -> String
dotFileGen world = constructDotGraph $ graphFromWorld world

webPageTemplate :: T.Text
webPageTemplate =
    T.unlines [ "<html>"
    , "<head><title>Daml Visualization</title><meta charset=\"utf-8\"></head>"
    , "<body>"
    , "<div id='viz'></div>"
    , "<script>"
    , "{{{d3Js}}}"
    , "</script>"
    , "<script>"
    , "{{{d3PlusJs}}}"
    , "</script>"
    , "<script>"
    , "var nodes = {{{dnodes}}}"
    , "var links = {{{links}}}"
    , "d3plus.viz()"
    , "          .container('#viz')"
    , "          .type('network')"
    , "          .data(nodes)"
    , "          .text('chcName')"
    , "          .edges({ value: links, arrows: true })"
    , "          .tooltip({"
    , "             Template: function (d) { return d['tplName'] },"
    , "             Fields: function (d) { return d['fields']; }"
    , "          })"
    , "          .draw();"
    , "</script>"
    , "</body>"
    , "</html>"
    ]

type OpenBrowserFlag = Bool
execVisualHtml :: FilePath -> FilePath -> OpenBrowserFlag -> IO ()
execVisualHtml darFilePath webFilePath oBrowser = do
    darBytes <- B.readFile darFilePath
    dalfs <- either fail pure $
                readDalfs $ ZIPArchive.toArchive (BSL.fromStrict darBytes)
    staticDir <- locateRunfiles $ "static_asset_d3plus" </> "js"
    d3js <-   readFile $ staticDir </> "d3.min.js"
    d3plusjs <- readFile $ staticDir </> "d3plus.min.js"
    let world = darToWorld dalfs
        graph = graphFromWorld world
        d3G = graphToD3Graph graph
        linksJson = DT.decodeUtf8 $ BSL.toStrict $ encode $ d3links d3G
        nodesJson = DT.decodeUtf8 $ BSL.toStrict $ encode $ d3nodes d3G
        webPage = WebPage linksJson nodesJson d3js d3plusjs
    case compileMustacheText "Webpage" webPageTemplate of
        Left err -> error $ show err
        Right mTpl -> do
            TIO.writeFile webFilePath $ renderMustache mTpl $ toJSON webPage
            when oBrowser
                (do _ <- openBrowser webFilePath
                    return ())


execVisual :: FilePath -> Maybe FilePath -> IO ()
execVisual darFilePath dotFilePath = do
    darBytes <- B.readFile darFilePath
    dalfs <- either fail pure $ readDalfs $ ZIPArchive.toArchive (BSL.fromStrict darBytes)
    let world = darToWorld dalfs
        result = dotFileGen world
    case dotFilePath of
        Just outDotFile -> writeFile outDotFile result
        Nothing -> putStrLn result
