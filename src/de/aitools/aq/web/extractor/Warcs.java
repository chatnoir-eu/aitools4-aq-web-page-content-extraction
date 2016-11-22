package de.aitools.aq.web.extractor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cmu.lemurproject.WarcRecord;

/**
 * Utility class for handling WARC files.
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date$
 *
 */
public class Warcs {
  
  private static final Pattern HTML_CONTENT_TYPE_PATTERN = Pattern.compile(
      "text/html.*");

  private static final String HEADER_START = "HTTP/";

  private static final String HEADER_CONTENT_TYPE = "Content-Type:";

  private static final Pattern HEADER_END_PATTERN = Pattern.compile(
      "\r?\n\r?\n");
  
  private Warcs() { }
  
  /**
   * Gets the HTML part of a record or <tt>null</tt> if there is none or an
   * invalid one.
   */
  public static String getHtml(final WarcRecord record) {
    if (!record.getHeaderRecordType().equals("response")) { return null; }

    final String warcContent = record.getContentUTF8().trim();

    final String httpHeader = Warcs.getHeader(warcContent);
    if (httpHeader == null) { return null; }

    final String contentType = Warcs.getContentType(httpHeader);
    if (contentType == null) { return null; }

    if (!HTML_CONTENT_TYPE_PATTERN.matcher(contentType).matches()) {
      return null;
    }
    
    return warcContent.substring(httpHeader.length());
  }

  private static String getHeader(final String warcContent) {
    if (!warcContent.startsWith(HEADER_START)) { return null; }

    final Matcher endMatcher = HEADER_END_PATTERN.matcher(warcContent);
    if (!endMatcher.find()) {
      return null;
    }
    final int httpHeaderEnd = endMatcher.end();

    return warcContent.substring(0, httpHeaderEnd);
  }

  private static String getContentType(final String httpHeader) {
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

}
