package de.aitools.aq.web.extractor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.entity.DecompressingEntity;
import org.apache.http.client.entity.DeflateInputStream;
import org.apache.http.client.entity.InputStreamFactory;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentLengthStrategy;
import org.apache.http.impl.entity.LaxContentLengthStrategy;
import org.apache.http.impl.io.ChunkedInputStream;
import org.apache.http.impl.io.ContentLengthInputStream;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.impl.io.EmptyInputStream;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.IdentityInputStream;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

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

  private static final String HEADER_CONTENT_TYPE = "Content-Type";

  private final static InputStreamFactory GZIP = new InputStreamFactory() {
    @Override
    public InputStream create(final InputStream instream) throws IOException {
      return new GZIPInputStream(instream);
    }
  };

  private final static InputStreamFactory DEFLATE = new InputStreamFactory() {
    @Override
    public InputStream create(final InputStream instream) throws IOException {
      return new DeflateInputStream(instream);
    }
  };
  
  private Warcs() { }
  
  public static Stream<WarcRecord> getRecords(final File input)
  throws IOException {
    final InputStream inputStream = new FileInputStream(input);
    if (Files.probeContentType(input.toPath()).equals("application/gzip")) {
      return Warcs.getRecords(new GZIPInputStream(inputStream));
    } else {
      return Warcs.getRecords(inputStream);
    }
  }
  
  public static Stream<WarcRecord> getRecords(final InputStream input)
  throws IOException {
    return Warcs.getRecords(new DataInputStream(input));
  }
  
  public static Stream<WarcRecord> getRecords(final DataInputStream input)
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
  
  public static Stream<String> getHtmlFromRecords(final File input)
  throws IOException {
    final InputStream inputStream = new FileInputStream(input);
    if (Files.probeContentType(input.toPath()).equals("application/gzip")) {
      return Warcs.getHtmlFromRecords(new GZIPInputStream(inputStream));
    } else {
      return Warcs.getHtmlFromRecords(inputStream);
    }
  }
  
  public static Stream<String> getHtmlFromRecords(final InputStream input)
  throws IOException {
    return Warcs.getHtmlFromRecords(new DataInputStream(input));
  }
  
  public static Stream<String> getHtmlFromRecords(final DataInputStream input)
  throws IOException {
    return Warcs.getRecords(input)
        .map(record -> {
            try {
              return Warcs.getHtml(record);
            } catch (final IOException e) {
              throw new UncheckedIOException(e);
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          })
        .filter(record -> record != null);
  }
  
  public static HttpResponse toResponse(final WarcRecord record)
  throws IOException, HttpException {
    // based on http://stackoverflow.com/a/26586178
    if (!record.getHeaderRecordType().equals("response")) { return null; }
    
    final SessionInputBufferImpl sessionInputBuffer =
        new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 2048);
    final InputStream inputStream =
        new ByteArrayInputStream(record.getByteContent());
    sessionInputBuffer.bind(inputStream);
    final DefaultHttpResponseParser parser =
        new DefaultHttpResponseParser(sessionInputBuffer);
    final HttpResponse response = parser.parse();
    final HttpEntity entity = Warcs.getEntity(response, sessionInputBuffer);
    response.setEntity(entity);
    Warcs.encodeEntity(response);
    return response;
  }
  
  private static void encodeEntity(final HttpResponse response)
  throws HttpException, IOException {
    // Adapted from org.apache.http.client.protocol.ResponseContentEncoding
    final HttpEntity entity = response.getEntity();
  
    // entity can be null in case of 304 Not Modified, 204 No Content or similar
    // check for zero length entity.
    if (entity != null && entity.getContentLength() != 0) {
      final Header ceheader = entity.getContentEncoding();
      if (ceheader != null) {
        final HeaderElement[] codecs = ceheader.getElements();
        final Lookup<InputStreamFactory> decoderRegistry =
            RegistryBuilder.<InputStreamFactory>create()
              .register("gzip", GZIP)
              .register("x-gzip", GZIP)
              .register("deflate", DEFLATE)
              .build();
        for (final HeaderElement codec : codecs) {
          final String codecname = codec.getName().toLowerCase(Locale.ROOT);
          final InputStreamFactory decoderFactory =
              decoderRegistry.lookup(codecname);
          if (decoderFactory != null) {
            response.setEntity(new DecompressingEntity(
                response.getEntity(), decoderFactory));
            response.removeHeaders("Content-Length");
            response.removeHeaders("Content-Encoding");
            response.removeHeaders("Content-MD5");
          } else {
            if (!"identity".equals(codecname)) {
                throw new HttpException(
                    "Unsupported Content-Encoding: " + codec.getName());
            }
          }
        }
      }
    }
}
  
  private static InputStream createInputStream(
      final long len, final SessionInputBuffer input) {
    // Adapted from the org.apache.http.impl.BHttpConnectionBase
    if (len == ContentLengthStrategy.CHUNKED) {
      return new ChunkedInputStream(input);
    } else if (len == ContentLengthStrategy.IDENTITY) {
      return new IdentityInputStream(input);
    } else if (len == 0L) {
      return EmptyInputStream.INSTANCE;
    } else {
      return new ContentLengthInputStream(input, len);
    }
  }
  
  private static HttpEntity getEntity(
      final HttpResponse response, final SessionInputBuffer input)
  throws HttpException {
    // Adapted from the org.apache.http.impl.BHttpConnectionBase
    final BasicHttpEntity entity = new BasicHttpEntity();

    final long len =
        LaxContentLengthStrategy.INSTANCE.determineLength(response);
    final InputStream instream = Warcs.createInputStream(len, input);
    if (len == ContentLengthStrategy.CHUNKED) {
      entity.setChunked(true);
      entity.setContentLength(-1);
      entity.setContent(instream);
    } else if (len == ContentLengthStrategy.IDENTITY) {
      entity.setChunked(false);
      entity.setContentLength(-1);
      entity.setContent(instream);
    } else {
      entity.setChunked(false);
      entity.setContentLength(len);
      entity.setContent(instream);
    }

    final Header contentTypeHeader = 
        response.getFirstHeader(HTTP.CONTENT_TYPE);
    if (contentTypeHeader != null) {
      entity.setContentType(contentTypeHeader);
    }
    final Header contentEncodingHeader =
        response.getFirstHeader(HTTP.CONTENT_ENCODING);
    if (contentEncodingHeader != null) {
      entity.setContentEncoding(contentEncodingHeader);
    }
    return entity;
  }
  
  /**
   * Gets the HTML part of a record or <tt>null</tt> if there is none or an
   * invalid one.
   * @throws IOException 
   * @throws ParseException 
   * @throws HttpException 
   */
  public static String getHtml(final WarcRecord record)
  throws ParseException, IOException, HttpException {
    final HttpResponse response = Warcs.toResponse(record);
    if (response == null) { return null; }
    final String contentType =
        response.getLastHeader(HEADER_CONTENT_TYPE).getValue();
    if (!HTML_CONTENT_TYPE_PATTERN.matcher(contentType).matches()) {
      return null;
    }
    final HttpEntity entity = response.getEntity();
    final String defaultCharset = null;
    return EntityUtils.toString(entity, defaultCharset);
  }
  
  public static void main(String[] args) throws IOException {
    System.out.println(Files.probeContentType(
        new File("/home/johannes/tmp/hpn/0000000177/WARCPROX-20161121092710357-00000-2228-pcstein4-8000.warc.gz").toPath()));
    System.out.println(Files.probeContentType(
        new File("/home/johannes/tmp/hpn/0000000177/WARCPROX-20161121092710357-00000-2228-pcstein4-8000.warc").toPath()));
    System.out.println(Files.probeContentType(
        new File("/home/johannes/tmp/content-extraction/input/quanta.htm").toPath()));
  }

}
