package de.aitools.aq.web.extractor;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
  
  public static Stream<WarcRecord> readRecords(final File input)
  throws IOException {
    return Warcs.readRecords(new FileInputStream(input));
  }
  
  public static Stream<WarcRecord> readRecords(final InputStream input)
  throws IOException {
    return Warcs.readRecords(new DataInputStream(input));
  }
  
  public static Stream<WarcRecord> readRecords(final DataInputStream input)
  throws IOException {
    final List<WarcRecord> records = new ArrayList<>();
    WarcRecord record = WarcRecord.readNextWarcRecord(input);
    while (record != null) {
      records.add(record);
      record = WarcRecord.readNextWarcRecord(input);
    }
    input.close();
    return records.stream();
  }
  
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
