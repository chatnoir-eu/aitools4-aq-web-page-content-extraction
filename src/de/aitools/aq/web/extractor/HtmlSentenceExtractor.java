package de.aitools.aq.web.extractor;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cmu.lemurproject.WarcRecord;

public abstract class HtmlSentenceExtractor {

  protected static final Pattern HTML_CONTENT_TYPE_PATTERN = Pattern.compile(
      "text/html.*");

  protected static final String HEADER_START = "HTTP/";

  protected static final String HEADER_CONTENT_TYPE = "Content-Type:";

  protected static final Pattern HEADER_END_PATTERN = Pattern.compile(
      "\r?\n\r?\n");
  

  public List<String> extractSentences(final WarcRecord record)
  throws IOException {
    if (!record.getHeaderRecordType().equals("response")) { return null; }

    final String warcContent = record.getContentUTF8().trim();

    final String httpHeader = this.getHeader(warcContent);
    if (httpHeader == null) { return null; }

    final String contentType = this.getContentType(httpHeader);
    if (contentType == null) { return null; }

    if (!HTML_CONTENT_TYPE_PATTERN.matcher(contentType).matches()) {
      return null;
    }
    
    final String httpContent = warcContent.substring(httpHeader.length());
    return this.extractSentences(httpContent);
  }

  public abstract List<String> extractSentences(final String htmlInput)
  throws NullPointerException, IllegalArgumentException;

  protected String getContentType(final String httpHeader) {
    final int fieldStart = httpHeader.indexOf(HEADER_CONTENT_TYPE);
    if (fieldStart < 0) { return null; }

    int fieldEnd = httpHeader.indexOf("\n", fieldStart);
    if (fieldEnd < 0) {
      fieldEnd = httpHeader.length();
    }

    final String fieldValue = httpHeader.substring(
        fieldStart + HEADER_CONTENT_TYPE.length(),
        fieldEnd);

    return fieldValue.trim().toLowerCase();
  }

  protected String getHeader(final String warcContent) {
    if (!warcContent.startsWith(HEADER_START)) { return null; }

    final Matcher endMatcher = HEADER_END_PATTERN.matcher(warcContent);
    if (!endMatcher.find()) {
      return null;
    }
    final int httpHeaderEnd = endMatcher.end();

    return warcContent.substring(0, httpHeaderEnd);
  }

}
