package com.iso20022.pain.generator;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Decorator that adds indentation (pretty-printing) to an
 * {@link XMLStreamWriter}.
 * <p>
 * Tracks element depth and inserts newline + indent whitespace before every
 * start element and after every end element. Leaf elements (elements that
 * contain only text) are kept on a single line for readability.
 * </p>
 */
final class IndentingXMLStreamWriter implements XMLStreamWriter {

    private final XMLStreamWriter delegate;
    private final String indent;
    private int depth = 0;
    private boolean hasText = false; // true when writeCharacters called inside an element

    IndentingXMLStreamWriter(XMLStreamWriter delegate, String indent) {
        this.delegate = delegate;
        this.indent = indent;
    }

    IndentingXMLStreamWriter(XMLStreamWriter delegate) {
        this(delegate, "  ");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void writeIndent() throws XMLStreamException {
        delegate.writeCharacters("\n");
        for (int i = 0; i < depth; i++) {
            delegate.writeCharacters(indent);
        }
    }

    // ── start / end elements ─────────────────────────────────────────────────

    @Override
    public void writeStartElement(String localName) throws XMLStreamException {
        writeIndent();
        delegate.writeStartElement(localName);
        depth++;
        hasText = false;
    }

    @Override
    public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException {
        writeIndent();
        delegate.writeStartElement(namespaceURI, localName);
        depth++;
        hasText = false;
    }

    @Override
    public void writeStartElement(String prefix, String localName, String namespaceURI) throws XMLStreamException {
        writeIndent();
        delegate.writeStartElement(prefix, localName, namespaceURI);
        depth++;
        hasText = false;
    }

    @Override
    public void writeEndElement() throws XMLStreamException {
        depth--;
        if (!hasText) {
            writeIndent();
        }
        delegate.writeEndElement();
        hasText = false;
    }

    @Override
    public void writeCharacters(String text) throws XMLStreamException {
        delegate.writeCharacters(text);
        hasText = true;
    }

    @Override
    public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
        delegate.writeCharacters(text, start, len);
        hasText = true;
    }

    // ── pass-through methods ─────────────────────────────────────────────────

    @Override
    public void writeStartDocument() throws XMLStreamException {
        delegate.writeStartDocument();
    }

    @Override
    public void writeStartDocument(String version) throws XMLStreamException {
        delegate.writeStartDocument(version);
    }

    @Override
    public void writeStartDocument(String encoding, String version) throws XMLStreamException {
        delegate.writeStartDocument(encoding, version);
    }

    @Override
    public void writeEndDocument() throws XMLStreamException {
        delegate.writeCharacters("\n");
        delegate.writeEndDocument();
    }

    @Override
    public void writeEmptyElement(String localName) throws XMLStreamException {
        writeIndent();
        delegate.writeEmptyElement(localName);
    }

    @Override
    public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException {
        writeIndent();
        delegate.writeEmptyElement(namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String prefix, String localName, String namespaceURI) throws XMLStreamException {
        writeIndent();
        delegate.writeEmptyElement(prefix, localName, namespaceURI);
    }

    @Override
    public void writeAttribute(String localName, String value) throws XMLStreamException {
        delegate.writeAttribute(localName, value);
    }

    @Override
    public void writeAttribute(String namespaceURI, String localName, String value) throws XMLStreamException {
        delegate.writeAttribute(namespaceURI, localName, value);
    }

    @Override
    public void writeAttribute(String prefix, String namespaceURI, String localName, String value)
            throws XMLStreamException {
        delegate.writeAttribute(prefix, namespaceURI, localName, value);
    }

    @Override
    public void writeNamespace(String prefix, String namespaceURI) throws XMLStreamException {
        delegate.writeNamespace(prefix, namespaceURI);
    }

    @Override
    public void writeDefaultNamespace(String namespaceURI) throws XMLStreamException {
        delegate.writeDefaultNamespace(namespaceURI);
    }

    @Override
    public void writeComment(String data) throws XMLStreamException {
        writeIndent();
        delegate.writeComment(data);
    }

    @Override
    public void writeProcessingInstruction(String target) throws XMLStreamException {
        delegate.writeProcessingInstruction(target);
    }

    @Override
    public void writeProcessingInstruction(String target, String data) throws XMLStreamException {
        delegate.writeProcessingInstruction(target, data);
    }

    @Override
    public void writeCData(String data) throws XMLStreamException {
        delegate.writeCData(data);
    }

    @Override
    public void writeDTD(String dtd) throws XMLStreamException {
        delegate.writeDTD(dtd);
    }

    @Override
    public void writeEntityRef(String name) throws XMLStreamException {
        delegate.writeEntityRef(name);
    }

    @Override
    public void close() throws XMLStreamException {
        delegate.close();
    }

    @Override
    public void flush() throws XMLStreamException {
        delegate.flush();
    }

    @Override
    public void setPrefix(String prefix, String uri) throws XMLStreamException {
        delegate.setPrefix(prefix, uri);
    }

    @Override
    public void setDefaultNamespace(String uri) throws XMLStreamException {
        delegate.setDefaultNamespace(uri);
    }

    @Override
    public void setNamespaceContext(NamespaceContext context) throws XMLStreamException {
        delegate.setNamespaceContext(context);
    }

    @Override
    public NamespaceContext getNamespaceContext() {
        return delegate.getNamespaceContext();
    }

    @Override
    public String getPrefix(String uri) throws XMLStreamException {
        return delegate.getPrefix(uri);
    }

    @Override
    public Object getProperty(String name) throws IllegalArgumentException {
        return delegate.getProperty(name);
    }
}
