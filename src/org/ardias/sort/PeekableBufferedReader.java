package org.ardias.sort;

import java.io.BufferedReader;
import java.io.IOException;

public class PeekableBufferedReader {

    public PeekableBufferedReader(BufferedReader r) throws IOException {
        this.reader = r;
        reload();
    }
    public void close() throws IOException {
        this.reader.close();
    }

    public boolean empty() {
        return this.lastRead == null;
    }

    public String peek() {
        return this.lastRead;
    }

    public String pop() throws IOException {
        String peek = peek();
        String ret = (peek != null ? peek.toString() : null);// make a copy
        reload();
        return ret;
    }

    private void reload() throws IOException {
        this.lastRead = this.reader.readLine();
    }

    public BufferedReader reader;

    private String lastRead;
}
