package com.mdburhan.comp8380.service;

import com.mdburhan.comp8380.domain.SearchResults;
import com.mdburhan.comp8380.domain.TitleAndPath;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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

    static int counter = 0;

    public String createIndex() throws IOException {
        Directory directory = FSDirectory.open(Paths.get(indexPath));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        indexDocs(indexWriter, Paths.get(documentPath));
        indexWriter.close();
        return "Index Stored at "+indexPath;
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
        InputStream stream = Files.newInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        String title = br.readLine();
        Document doc = new Document();
        doc.add(new StringField("path", file.toString(), Field.Store.YES));
        doc.add(new TextField("contents", br));
        doc.add(new StringField("title", title, Field.Store.YES));
        writer.addDocument(doc);
        counter++;
        if (counter % 1000 == 0)
            System.out.println("indexing " + counter + "-th file " + file.getFileName());
    }

    public SearchResults searchIndex(String queryString, int nMatches) throws IOException, ParseException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser parser = new QueryParser("contents", analyzer);
        Query query = parser.parse(queryString);
        TopDocs results = searcher.search(query, nMatches);

        SearchResults searchResults = new SearchResults();
        searchResults.setTotalHits( results.totalHits);
        List<TitleAndPath> titleAndPaths = new ArrayList<>();
        for (int i = 0; i < nMatches; i++) {
            Document doc = searcher.doc(results.scoreDocs[i].doc);
            TitleAndPath titleAndPath = new TitleAndPath();
            titleAndPath.setPath(doc.get("path"));
            titleAndPath.setTitle(doc.get("title")!=null?doc.get("title"):"title not found");
            titleAndPaths.add(titleAndPath);
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
        searchResults.setMatches(titleAndPaths);
        return searchResults;
    }
    public SearchResults phraseSearch(String queryString,int slop , int nMatches) throws IOException, ParseException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser parser = new QueryParser("contents", analyzer);
        queryString = "\""+queryString+"\"~"+slop;

        Query query = parser.parse(queryString);
        TopDocs results = searcher.search(query, nMatches);

        SearchResults searchResults = new SearchResults();
        searchResults.setTotalHits( results.totalHits);
        List<TitleAndPath> titleAndPaths = new ArrayList<>();
        for (int i = 0; i < nMatches; i++) {
            Document doc = searcher.doc(results.scoreDocs[i].doc);
            TitleAndPath titleAndPath = new TitleAndPath();
            titleAndPath.setPath(doc.get("path"));
            titleAndPath.setTitle(doc.get("title")!=null?doc.get("title"):"title not found");
            titleAndPaths.add(titleAndPath);
        }
        reader.close();
        searchResults.setMatches(titleAndPaths);
        return searchResults;
    }

    /**without a query parser*/
    public SearchResults phraseSearchV2(String queryString, int slop, int nMatches) throws IOException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher searcher = new IndexSearcher(reader);
        queryString = queryString.toLowerCase();
        String [] terms = queryString.split(" ");
        PhraseQuery query = new PhraseQuery(0, "contents", terms);
        TopDocs results = searcher.search(query, nMatches);
        SearchResults searchResults = new SearchResults();
        searchResults.setTotalHits( results.totalHits);
        List<TitleAndPath> titleAndPaths = new ArrayList<>();
        for (int i = 0; i < nMatches; i++) {
            Document doc = searcher.doc(results.scoreDocs[i].doc);
            TitleAndPath titleAndPath = new TitleAndPath();
            titleAndPath.setPath(doc.get("path"));
            titleAndPath.setTitle(doc.get("title")!=null?doc.get("title"):"title not found");
            titleAndPaths.add(titleAndPath);
        }
        reader.close();
        searchResults.setMatches(titleAndPaths);
        return searchResults;

    }
}
