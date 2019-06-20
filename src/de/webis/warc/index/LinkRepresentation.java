package de.webis.warc.index;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LinkRepresentation {

    /////////////////////////////////////////////////////////////////////////////
    // MEMBERS
    /////////////////////////////////////////////////////////////////////////////

    private String url;

    private String context;

    private String original;

    /////////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    /////////////////////////////////////////////////////////////////////////////

    public LinkRepresentation() {
        this("", "", "");
    }

    public LinkRepresentation(String url, String context, String original) {
        this.url = url;
        this.context = context;
        this.original = original;
    }

    /////////////////////////////////////////////////////////////////////////////
    // SETTERS
    /////////////////////////////////////////////////////////////////////////////

    public void setUrl(String url) {
        this.url = url;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public void setOriginal(String original) {
        this.original = original;
    }

    /////////////////////////////////////////////////////////////////////////////
    // GETTERS
    /////////////////////////////////////////////////////////////////////////////

    public String getUrl() {
        return url;
    }

    public String getContext() {
        return context;
    }

    public String getOriginal() {
        return original;
    }

    /////////////////////////////////////////////////////////////////////////////
    // FUNCTIONALITY
    /////////////////////////////////////////////////////////////////////////////

    public String toString() {
        return String.format("\"%s\" <%s> (from %s)", this.context, this.url, this.original);
    }
}