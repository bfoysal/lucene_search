package com.mdburhan.comp8380.service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.mdburhan.comp8380.domain.SearchResults;
import com.mdburhan.comp8380.domain.Paper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.print.Doc;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author burhan <burhan420@gmail.com>
 * @project comp8380
 * @created at 2020-02-04
 */
@Service
public class LuceneService {
    @Value("${document.path}")
    private String documentPath;
    @Value("${index.path}")
    private String indexPath;
    @Value(("${rank.path}"))
    private String pageRankPath;

    private static final String TITLE = "title";
    private static final String ABSTRACT = "abstract";
    private static final String CONTENTS = "contents";
    private static final String ID = "id";
    private static final String PAGE_RANK = "pr";
    static int counter = 0;
    static Map<String, Double> pageRanks=new HashMap<String, Double>();

    public String createIndex() throws IOException {
        pageRanks = getPageRank(pageRankPath+"/page_ranks.txt");
        Directory directory = FSDirectory.open(Paths.get(indexPath));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        indexDocs(indexWriter, Paths.get(documentPath));
        indexWriter.close();
        return "Index Stored at "+indexPath;
    }

    private Map<String, Double> getPageRank(String s) throws IOException {
        Map<String,Double> rankMap = new HashMap<String, Double>();
        InputStream stream = Files.newInputStream(Paths.get(s));
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        String line;
        while((line=br.readLine())!=null){
            String []  pair = line.split(" ");
            String key = pair[0];
            double rank = Double.parseDouble(pair[1]);
            rankMap.put(key,rank);
        }
        return rankMap;
    }

    static void indexDocs(final IndexWriter writer, Path path) throws IOException {

        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                indexDoc(writer, file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /** Indexes a single document */
    static void indexDoc(IndexWriter writer, Path file) throws IOException {
//        InputStream stream = Files.newInputStream(file);
//        BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
//        String title = br.readLine();
        Document doc = new Document();
        /*FieldType fieldType =new FieldType();
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorOffsets(true);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        doc.add(new Field(contents,br,fieldType));*/
        //trying json streaming
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser = jsonFactory.createParser(file.toFile());
        String titleString="", abstractString="" ,fieldName="", idString="";
        JsonToken token;
        while (!jsonParser.isClosed()){
            token = jsonParser.nextToken();
            fieldName = jsonParser.getCurrentName();
            if(JsonToken.FIELD_NAME.equals(token)){
                if (fieldName.equals(ABSTRACT)){
                    jsonParser.nextToken();
                    abstractString=jsonParser.getValueAsString();
                }
                if (fieldName.equals(TITLE)){
                    jsonParser.nextToken();
                    titleString=jsonParser.getValueAsString();
                }
                if (fieldName.equals(ID)){
                    jsonParser.nextToken();
                    idString=jsonParser.getValueAsString();
                }
            }
            if(JsonToken.END_OBJECT.equals(token)){
                doc.add(new StringField(TITLE,titleString,Field.Store.YES));
                doc.add(new StringField(ABSTRACT,abstractString,Field.Store.YES));
                doc.add(new DoubleDocValuesField(PAGE_RANK,pageRanks.get(idString)));
                FieldType fieldType =new FieldType();
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorOffsets(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                doc.add(new Field(CONTENTS,titleString+abstractString,fieldType));
                writer.addDocument(doc);
                doc=new Document();
                titleString=abstractString=idString="";
                counter++;
                if (counter % 1000 == 0){
                    System.out.println("indexing "+counter+"-th document");
                }

            }
        }
        //trying json streaming

        /*doc.add(new StringField("path", file.toString(), Field.Store.YES));
        doc.add(new TextField("contents", br));
        doc.add(new StringField("title", title, Field.Store.YES));
        writer.addDocument(doc);*/
        /*counter++;
        if (counter % 1000 == 0)
            System.out.println("indexing " + counter + "-th file " + file.getFileName());*/
    }

    public SearchResults searchIndex(String queryString, int nMatches) throws IOException, ParseException, InvalidTokenOffsetsException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser parser = new QueryParser(CONTENTS, analyzer);
        Query query = parser.parse(queryString);
//        System.out.println(query.toString());

        Sort sortByPR = new Sort(new SortField(PAGE_RANK, SortField.Type.DOUBLE));
        TopDocs results = searcher.search(query, nMatches,sortByPR);
//        TopDocs results = searcher.search(query, nMatches);

        SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter();
        Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(query));

        SearchResults searchResults = new SearchResults();
        searchResults.setTotalHits( results.totalHits);
        List<Paper> papers = new ArrayList<>();

        for (int i = 0; i < nMatches; i++) {
            int id = results.scoreDocs[i].doc;
            Document doc = searcher.doc(id);
            Paper paper = new Paper();
//            paper.setPr(doc.get(PAGE_RANK));
            String title = doc.get(TITLE);
            Fields fields = reader.getTermVectors(id);
            TokenStream titleStream = TokenSources.getTokenStream(TITLE,fields,title,analyzer,-1);
//            TokenStream tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(),id,TITLE, analyzer);
            TextFragment [] fragments = highlighter.getBestTextFragments(titleStream,title,true, 10);
            String highlightResult ="";
            for (int j=0; j < fragments.length; j++){
                if(fragments[j]!=null){
                    highlightResult+=fragments[j].toString();
                }
            }
            paper.setTitle(highlightResult);
            TokenStream abstractStream =TokenSources.getTokenStream(ABSTRACT,fields,doc.get(ABSTRACT),analyzer,-1);
            fragments = highlighter.getBestTextFragments(abstractStream,doc.get(ABSTRACT),true,10);
            highlightResult = "";
            for (int j = 0; j < fragments.length; j++) {
                if (fragments[j]!=null){
                    highlightResult+=fragments[j].toString();
                }
            }
            paper.setAbs(highlightResult);

            papers.add(paper);
        }
        /*System.out.println(results.totalHits + " total matching documents");
        for (int i = 0; i < 5; i++) {
            Document doc = searcher.doc(results.scoreDocs[i].doc);
            String path = doc.get("path");
            System.out.println((i + 1) + ". " + path);
            String title = doc.get("title");
            if (title != null) {
                System.out.println("   Title: " + doc.get("title"));
            }
        }*/
        reader.close();
        searchResults.setMatches(papers);
        return searchResults;
    }
    public SearchResults phraseSearch(String queryString,int slop , int nMatches) throws IOException, ParseException, InvalidTokenOffsetsException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser parser = new QueryParser(CONTENTS, analyzer);
        queryString = "\""+queryString+"\"~"+slop;
        Query query = parser.parse(queryString);
//        System.out.println(query.toString());
        Sort sortByPR = new Sort(new SortField(PAGE_RANK, SortField.Type.DOUBLE));
        TopDocs results = searcher.search(query, nMatches,sortByPR);
//        TopDocs results = searcher.search(query, nMatches);

        SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter();
        Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(query));

        SearchResults searchResults = new SearchResults();
        searchResults.setTotalHits( results.totalHits);
        List<Paper> papers = new ArrayList<>();

        for (int i = 0; i < nMatches; i++) {
            int id = results.scoreDocs[i].doc;
            Document doc = searcher.doc(id);
            Paper paper = new Paper();
            //paper.setAbs(doc.get(ABSTRACT));
            String title = doc.get(TITLE);
            Fields fields = reader.getTermVectors(id);
            TokenStream titleStream = TokenSources.getTokenStream(TITLE,fields,title,analyzer,-1);
//            TokenStream tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(),id,TITLE, analyzer);
            TextFragment [] fragments = highlighter.getBestTextFragments(titleStream,title,true, 10);
            String highlightResult ="";
            for (int j=0; j < fragments.length; j++){
                if(fragments[j]!=null){
                    highlightResult+=fragments[j].toString();
                }
            }
            paper.setTitle(highlightResult);
            TokenStream abstractStream =TokenSources.getTokenStream(ABSTRACT,fields,doc.get(ABSTRACT),analyzer,-1);
            fragments = highlighter.getBestTextFragments(abstractStream,doc.get(ABSTRACT),true,10);
            highlightResult = "";
            for (int j = 0; j < fragments.length; j++) {
                if (fragments[j]!=null){
                    highlightResult+=fragments[j].toString();
                }
            }
            paper.setAbs(highlightResult);

            papers.add(paper);
        }
        reader.close();
        searchResults.setMatches(papers);
        return searchResults;
    }

    /**without a query parser*/
    public SearchResults phraseSearchV2(String queryString, int slop, int nMatches) throws IOException, InvalidTokenOffsetsException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        queryString = queryString.toLowerCase();
        String [] terms = queryString.split(" ");
        PhraseQuery query = new PhraseQuery(slop, CONTENTS, terms);
        TopDocs results = searcher.search(query, nMatches);

        SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter();
        Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(query));

        SearchResults searchResults = new SearchResults();
        searchResults.setTotalHits( results.totalHits);
        List<Paper> papers = new ArrayList<>();

        for (int i = 0; i < nMatches; i++) {
            int id = results.scoreDocs[i].doc;
            Document doc = searcher.doc(id);
            Paper paper = new Paper();
            //paper.setAbs(doc.get(ABSTRACT));
            String title = doc.get(TITLE);
            Fields fields = reader.getTermVectors(id);
            TokenStream titleStream = TokenSources.getTokenStream(TITLE,fields,title,analyzer,-1);
//            TokenStream tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(),id,TITLE, analyzer);
            TextFragment [] fragments = highlighter.getBestTextFragments(titleStream,title,true, 10);
            String highlightResult ="";
            for (int j=0; j < fragments.length; j++){
                if(fragments[j]!=null){
                    highlightResult+=fragments[j].toString();
                }
            }
            paper.setTitle(highlightResult);
            TokenStream abstractStream =TokenSources.getTokenStream(ABSTRACT,fields,doc.get(ABSTRACT),analyzer,-1);
            fragments = highlighter.getBestTextFragments(abstractStream,doc.get(ABSTRACT),true,10);
            highlightResult = "";
            for (int j = 0; j < fragments.length; j++) {
                if (fragments[j]!=null){
                    highlightResult+=fragments[j].toString();
                }
            }
            paper.setAbs(highlightResult);

            papers.add(paper);
        }
        reader.close();
        searchResults.setMatches(papers);
        return searchResults;

    }
}


    /*String line;
        while ((line=br.readLine())!=null){
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES,false);
                DBLPv10 dblpv10 = objectMapper.readValue(line,DBLPv10.class);
        doc.add(new StringField("title", dblpv10.getTitle(),Field.Store.YES));
        doc.add(new StringField("abstract",dblpv10.getAbs(),Field.Store.YES));
        FieldType fieldType =new FieldType();
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorOffsets(true);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        doc.add(new Field("contents",dblpv10.paper(),fieldType));
        counter++;
        if (counter % 1000 == 0)
        System.out.println("indexing " + counter + "-th file " + file.getFileName());
        }*/