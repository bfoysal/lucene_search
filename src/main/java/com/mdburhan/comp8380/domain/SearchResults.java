package com.mdburhan.comp8380.domain;

import java.util.List;

/**
 * @author burhan <burhan420@gmail.com>
 * @project comp8380
 * @created at 2020-02-04
 */
public class SearchResults {
    private Long totalHits;
    private List matches;

    public Long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(Long totalHits) {
        this.totalHits = totalHits;
    }

    public List getMatches() {
        return matches;
    }

    public void setMatches(List matches) {
        this.matches = matches;
    }
}