package com.mdburhan.comp8380.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author burhan <burhan420@gmail.com>
 * @project comp8380
 * @created at 2020-02-04
 */
public class Paper {
    private String title;
    @JsonProperty("abstract")
    private String abs;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAbs() {
        return abs;
    }

    public void setAbs(String abs) {
        this.abs = abs;
    }
}
