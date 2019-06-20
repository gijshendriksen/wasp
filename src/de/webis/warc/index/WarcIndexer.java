package de.webis.warc.index;

import java.io.IOException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import net.htmlparser.jericho.*;
import org.apache.http.HttpResponse;
import org.joda.time.Instant;

import de.webis.warc.Warcs;
import de.webis.warc.read.ArchiveWatcher;
import edu.cmu.lemurproject.WarcRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WarcIndexer implements Consumer<WarcRecord> {

  /////////////////////////////////////////////////////////////////////////////
  // LOGGING
  /////////////////////////////////////////////////////////////////////////////
  
  private static final Logger LOG =
      Logger.getLogger(WarcIndexer.class.getName());
  
  /////////////////////////////////////////////////////////////////////////////
  // DEFAULTS
  /////////////////////////////////////////////////////////////////////////////
  
  public static final Function<Source, String> DEFAULT_HTML_CONTENT_EXTRACTOR =
      source -> {
        final Renderer renderer = new Renderer(source);
        renderer.setMaxLineLength(0);
        renderer.setIncludeHyperlinkURLs(false);
        renderer.setIncludeAlternateText(true);
        return renderer.toString();
      };
  
  /////////////////////////////////////////////////////////////////////////////
  // MEMBERS
  /////////////////////////////////////////////////////////////////////////////
  
  protected final Index index;
  
  protected final Function<Source, String> htmlContentExtractor;

  protected final Path crawlUrlPath;
  
  /////////////////////////////////////////////////////////////////////////////
  // CONSTRUCTORS
  /////////////////////////////////////////////////////////////////////////////
  
  public WarcIndexer(final Index index, final Path crawlUrlPath) {
    this(index, crawlUrlPath, DEFAULT_HTML_CONTENT_EXTRACTOR);
  }
  
  public WarcIndexer(
      final Index index,
      final Path crawlUrlPath,
      final Function<Source, String> htmlContentExtractor) {
    if (index == null) { throw new NullPointerException("index"); }
    if (htmlContentExtractor == null) {
      throw new NullPointerException("HTML Content Extractor");
    }
    
    this.index = index;
    this.crawlUrlPath = crawlUrlPath;
    this.htmlContentExtractor = htmlContentExtractor;
  }
  
  /////////////////////////////////////////////////////////////////////////////
  // FUNCTIONALITY
  /////////////////////////////////////////////////////////////////////////////

  @Override
  public void accept(final WarcRecord record) {
    try {
      this.index(record);
    } catch (final IOException exception) {
      LOG.log(Level.WARNING, "Failed to index record " + Warcs.getId(record)
          + " of type " + Warcs.getType(record), exception);
    }
  }

  public void index(final WarcRecord record)
  throws IOException {
    final String type = Warcs.getType(record);
    switch (type) {
    case Warcs.HEADER_TYPE_RESPONSE:
      this.indexResponse(record);
      break;
    case Warcs.HEADER_TYPE_REQUEST:
      this.indexRequest(record);
      break;
    case Warcs.HEADER_TYPE_REVISIT:
      this.indexRevisit(record);
      break;
    default:
      break;
    }
  }
  
  /////////////////////////////////////////////////////////////////////////////
  // Response
  
  protected void indexResponse(final WarcRecord record)
  throws IOException {
    final String id = Warcs.getId(record);
    final String uri = Warcs.getTargetUri(record);
    final Source source = this.getSource(record);
    if (source != null) {
      final String content = this.getContent(source);
      if (content != null) {
        final String title = this.getTitle(source);
        this.index.indexResponse(id, content, title);
        this.storeCrawlUrls(source, uri, id);
      }
    }
  }

  protected Source getSource(final WarcRecord record) {
    try {
      final HttpResponse response = Warcs.toResponse(record);
      if (Warcs.isHtml(response)) {
        final String html = Warcs.getHtml(record);
        return new Source(html);
      }
    } catch (final Throwable exception) {
      LOG.log(Level.FINER,
          "Could not parse record " + Warcs.getId(record),
          exception);
    }
    return null;
  }
  
  protected String getContent(final Source source) {
    try {
      return this.htmlContentExtractor.apply(source);
    } catch (final Throwable exception) {
      LOG.log(Level.FINER,
          "Could not parse source",
          exception);
    }
    return null;
  }
  
  protected String getTitle(final Source source) {
    final Element title = source.getFirstElement(HTMLElementName.TITLE);
    if (title == null) {
      return "";
    } else {
      return CharacterReference.decodeCollapseWhiteSpace(title.getContent());
    }
  }

  /**
   * Extracts URLs from the source document, and corresponding context for each of them.
   * @param source the HTML source of the indexed document
   * @param originalUri the URI of the indexed document
   * @param documentId the id of the indexed document
   */
  private void storeCrawlUrls(final Source source, final String originalUri, final String documentId) {
    List<Element> elements = source.getAllElements("a");

    try {
      List<LinkRepresentation> currentUrls = new ArrayList<>();
      File file = this.crawlUrlPath.toFile();

      ObjectMapper mapper = new ObjectMapper();

      // If the file containing URLs already exists, extract its contents in order to avoid duplicates.
      if (!file.createNewFile()) {
        currentUrls = mapper.readValue(file, new TypeReference<List<LinkRepresentation>>() {});
      }

      for (Element el : elements) {
        final String href = el.getAttributeValue("href");
        final String parsedUrl = this.parseUrl(href, originalUri);

        if (parsedUrl != null && !containsUrl(currentUrls, parsedUrl)) {
          final String context = this.getContext(el, 40);

          LinkRepresentation repr = new LinkRepresentation(parsedUrl, context, documentId);

          currentUrls.add(repr);
        }
      }

      mapper.writeValue(file, currentUrls);
    } catch (IOException e) {
      LOG.log(Level.FINE,
              "Could not add URLs to the crawl index",
              e);
    }
  }

  /**
   * Extracts the context of the given anchor tag. In other words, extracts the surrounding words of the
   * anchor tag to a maximum of the specified amount of words.
   *
   * @param element the Element representation of the anchor tag
   * @param contextLength the amount of words to find inside and around the anchor tag
   * @return the context of the anchor tag as a string of words, consisting only of lower case letters, numbers and spaces.
   */
  private String getContext(Element element, int contextLength) {
    TextExtractor extractor = element.getTextExtractor();
    // The words inside the anchor tag
    String[] innerContext = extractor.toString().split("\\s+");
    String innerHtml = element.toString();

    // The amount of words to keep on either side of the anchor tag
    int rightAmount = (contextLength - innerContext.length) / 2;
    int leftAmount = contextLength - innerContext.length - rightAmount;

    String[] leftContext = {};
    String[] rightContext = {};

    // Container element used to traverse up the HTML tree
    Element parent = element;

    // Iterates until we have reached the root element or
    // we have gathered enough context on either side of the anchor tag
    while ((leftContext.length < leftAmount || rightContext.length < rightAmount) &&
            (parent = parent.getParentElement()) != null) {
      String[] parts = parent.getTextExtractor().toString().split(innerHtml);

      if (leftContext.length < leftAmount) {
        // Extract context to the left of the anchor tag
        String[] left = parts[0].trim().split("\\s+");
        leftContext = Arrays.copyOfRange(left, left.length - leftAmount, left.length);
      }

      if (rightContext.length < rightAmount) {
        // Extract context to the right of the anchor tag
        String[] right = parts[1].trim().split("\\s+");
        rightContext = Arrays.copyOfRange(right, 0, rightAmount);
      }
    }

    // Combine all three parts and remove special characters
    String finalContext = String.join(" ", leftContext) + " " +
                          String.join(" ", innerContext) + " " +
                          String.join(" ", rightContext);
    return finalContext.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase();
  }

  /**
   * Converts any URL to the correct absolute URL.
   * @param url the URL or path to parse
   * @param originalUrl the URL of the page on which the target URL is found (as context)
   * @return the parsed and converted URL, or null if no valid URL could be extracted
   */
  private String parseUrl(String url, String originalUrl) {
    if (url == null) {
      return null;
    }

    // Remove any targets from the URL as they do not matter for the purposes of retrieving the page
    String[] parts = url.split("#");
    if (parts.length == 0) {
      return null;
    }

    // Check whether the anchor location is actually a URL, and not just an empty location or a javascript call
    url = parts[0].trim();
    if (url.isEmpty() || url.startsWith("javascript:")) {
      return null;
    }

    try {
      // Use the original URL to provide context for the target URL.
      // The java.net library does all the heavy lifting.
      URL finalUrl = new URL(new URL(originalUrl), url);

      // No local websites, obviously.
      if (finalUrl.getHost().equals("localhost")) {
        return null;
      }

      return finalUrl.toString();
    } catch (MalformedURLException e) {
      LOG.log(Level.FINE,
              "Could not parse URL " + url + " found in " + originalUrl);
      return null;
    }
  }

  private boolean containsUrl(List<LinkRepresentation> list, String url) {
    for (LinkRepresentation repr : list) {
      if (url.equalsIgnoreCase(repr.getUrl())) {
        return true;
      }
    }
    return false;
  }
  
  /////////////////////////////////////////////////////////////////////////////
  // Revisit
  
  protected void indexRevisit(final WarcRecord record)
  throws IOException {
    this.index.indexRevisit(
        Warcs.getId(record),
        Warcs.getReferedToRecordId(record));
  }
  
  /////////////////////////////////////////////////////////////////////////////
  // Request
  
  protected void indexRequest(final WarcRecord record) throws IOException {
    this.index.indexRequest(
        Warcs.getConcurrentRecordId(record),
        Warcs.getTargetUri(record),
        new Instant(Warcs.getDate(record).getEpochSecond() * 1000));
  }
  
  /////////////////////////////////////////////////////////////////////////////
  // MAIN
  /////////////////////////////////////////////////////////////////////////////
  
  public static void main(final String[] args) throws IOException {
    final ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINE);
    final Logger logger = Logger.getLogger("de.webis.warc");
    logger.addHandler(handler);
    logger.setLevel(Level.FINE);
    
    final Path archiveDirectory = Paths.get(args[0]);
    final Path crawlUrlPath = Paths.get(args[1]);
    final int port =
        args.length != 3 ? Index.DEFAULT_PORT : Integer.parseInt(args[2]);
    try (final Index index = new Index(port)) {
      final WarcIndexer indexer = new WarcIndexer(index, crawlUrlPath);
      
      try (final ArchiveWatcher watcher =
          new ArchiveWatcher(archiveDirectory, false, indexer)) {
        watcher.run();
      }
    }
  }
  
}
