package com.invertedindex;

import org.apache.hadoop.io.Text;

public class WordMetaData {

    private final Text fileName;
    private final Text lineNumber;

    public WordMetaData() {
        fileName = new Text();
        lineNumber = new Text();
    }

    public WordMetaData(Text fileName, Text lineNumber) {
        this.fileName = fileName;
        this.lineNumber = lineNumber;
    }

    public Text getFileName() {
        return fileName;
    }

    public Text getLineNumber() {
        return lineNumber;
    }

    @Override
    public String toString() {
        return "{" +
                "fileName=" + fileName.toString() +
                "_lineNumber=" + lineNumber.toString() +
                '}';
    }

}
