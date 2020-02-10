package com.mdburhan.comp8380.controller;

import com.mdburhan.comp8380.service.LuceneService;
import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

/**
 * @author burhan <burhan420@gmail.com>
 * @project comp8380
 * @created at 2020-02-03
 */
@CrossOrigin
@RestController
@RequestMapping(value = "/lucene")
public class ApiController {

    @Autowired
    private LuceneService service;

    @GetMapping(value = "/create_index")
    public ResponseEntity<Object> createIndex() throws IOException {
        return ResponseEntity.ok(service.createIndex());
    }
    @GetMapping(value = "/search/{query}/{nMatches}")
    public ResponseEntity<Object> searchIndex(@PathVariable(name = "query") String queryString, @PathVariable(name = "nMatches") int nMatches) throws IOException, ParseException {
        return ResponseEntity.ok(service.searchIndex(queryString, nMatches));
    }
    @GetMapping(value = "/phrase_search/{query}/{slop}/{nMatches}")
    public ResponseEntity<Object> searchPhrase(@PathVariable(name = "query") String queryString, @PathVariable(name = "slop")int slop, @PathVariable("nMatches") int nMatches) throws IOException, ParseException {
        return ResponseEntity.ok(service.phraseSearch(queryString,slop,nMatches));
    }
    @GetMapping(value = "/phrase_search_v2/{query}/{slop}/{nMatches}")
    public ResponseEntity<Object> searchPhraseV2(@PathVariable(name = "query") String queryString, @PathVariable(name = "slop")int slop, @PathVariable("nMatches") int nMatches) throws IOException, ParseException {
        return ResponseEntity.ok(service.phraseSearchV2(queryString,slop,nMatches));
    }
}
