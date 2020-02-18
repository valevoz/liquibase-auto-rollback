package org.v5k.liquibase;

public class Rollback {
    private final String id;
    private final String author;
    private final String filename;
    private final String script;

    public Rollback(String id, String author, String filename, String script) {
        this.id = id;
        this.author = author;
        this.filename = filename;
        this.script = script;
    }

    public String getId() {
        return id;
    }

    public String getAuthor() {
        return author;
    }

    public String getFilename() {
        return filename;
    }

    public String getScript() {
        return script;
    }
}
