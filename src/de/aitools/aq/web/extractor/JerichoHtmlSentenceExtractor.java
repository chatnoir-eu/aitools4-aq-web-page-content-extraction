package de.aitools.aq.web.extractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.ibm.icu.text.BreakIterator;

import de.aitools.ie.languagedetection.LanguageDetector;
import net.htmlparser.jericho.Renderer;
import net.htmlparser.jericho.Segment;
import net.htmlparser.jericho.Source;

public class JerichoHtmlSentenceExtractor extends HtmlSentenceExtractor {
  
  private static String FLAG_INPUT = "input_files";
  
  private static String FLAG_OUTPUT = "output_directory";
  
  private static String FLAG_EXTRACT_ALL_LANGUAGES = "language_extract_all";
  
  private static String FLAG_EXTRACT_LANGUAGES = "language_extract";
  
  private static String FLAG_USE_LANGUAGE = "language_use";
  
  private static String FLAG_DO_NOT_SEPARATE_PARAGRAPHS = "separate_paragraphs_not";
  
  private static String FLAG_PARAGRAPH_SEPARATOR = "separate_paragraph_with";

  private Set<Locale> targetLanguages;

  private Function<String, Locale> languageDetector;
  
  private String paragraphSeparator;
  
  private boolean separateParagraphs;
  
  public JerichoHtmlSentenceExtractor() {
    this.setExtractLanguage(Locale.ENGLISH);
    this.setDoNotSeparateParagraphs();
  }

  public Set<Locale> getTargetLanguages() {
    return Collections.unmodifiableSet(this.targetLanguages);
  }
  
  public Function<String, Locale> getLanguageDetector() {
    return this.languageDetector;
  }
  
  public String getParagraphSeparator() {
    return this.paragraphSeparator;
  }
  
  public void setExtractAllLanguages() {
    this.targetLanguages = null;
    final LanguageDetector languageDetector = new LanguageDetector();
    this.setLanguageDetector(text -> languageDetector.detect(text));
  }

  public void setExtractLanguages(final String... targetLanguages) {
    final List<Locale> locales = new ArrayList<>(targetLanguages.length);
    for (final String targetLanguage : targetLanguages) {
      locales.add(Locale.forLanguageTag(targetLanguage));
    }
    this.setExtractLanguages(locales);
  }

  public void setExtractLanguages(final Locale... targetLanguages) {
    this.setExtractLanguages(Arrays.asList(targetLanguages));
  }

  public void setExtractLanguages(final Collection<Locale> targetLanguages) {
    this.targetLanguages = new HashSet<>(targetLanguages);
    final LanguageDetector languageDetector = new LanguageDetector();
    this.setLanguageDetector(text -> languageDetector.detect(text));
  }
  
  public void setExtractLanguage(final Locale targetLanguage) {
    this.setExtractLanguages(Collections.singleton(targetLanguage));
  }
  
  public void setUseLanguage(final String language) {
    this.setUseLanguage(Locale.forLanguageTag(language));
  }
  
  public void setUseLanguage(final Locale language) {
    if (language == null) { throw new NullPointerException(); }
    this.setExtractLanguage(language);
    this.setLanguageDetector(text -> language);
  }
  
  public void setLanguageDetector(
      final Function<String, Locale> languageDetector)
  throws NullPointerException {
    if (languageDetector == null) { throw new NullPointerException(); }
    this.languageDetector = languageDetector;
  }
  
  public void setDoNotSeparateParagraphs() {
    this.setParagraphSeparator(null);
    this.separateParagraphs = false;
  }

  public void setParagraphSeparator(final String paragraphSeparator) {
    this.paragraphSeparator = paragraphSeparator;
    this.separateParagraphs = true;
  }

  @Override
  public List<String> extractSentences(final String htmlInput)
  throws NullPointerException, IllegalArgumentException {
    if (htmlInput == null) {
      throw new NullPointerException();
    }

    final List<String> paragraphs = this.extractParagraphs(htmlInput);
    if (paragraphs == null) {
      throw new IllegalArgumentException("Could not parse: " + htmlInput);
    }
    final List<String> sentences = new ArrayList<>();
    boolean firstParagraph = true;
    for (final String paragraph : paragraphs) {
      final List<String> paragraphSentences =
          this.extractSentencesFromParagraph(paragraph);
      if (!paragraphSentences.isEmpty()) {
        if (firstParagraph) {
          firstParagraph = false;
        } else if (this.separateParagraphs) {
          sentences.add(this.paragraphSeparator);
        }
        sentences.addAll(paragraphSentences);
      }
    }
    return sentences;
  }
  
  protected List<String> extractParagraphs(final String htmlInput) {
    try {
      final Source source = new Source(htmlInput);
      final Segment segment = new Segment(source, 0, htmlInput.length());
      final Renderer renderer = new Renderer(segment);
      renderer.setMaxLineLength(0);
      renderer.setIncludeHyperlinkURLs(false);
      final String[] paragraphsArray = renderer.toString().split("\n");
      final List<String> paragraphs = new ArrayList<>(paragraphsArray.length);
      for (final String paragraph : paragraphsArray) {
        paragraphs.add(this.normalizeWhitespace(paragraph));
      }
      return paragraphs;
    } catch (final Error error) {
      return null;
    }
  }

  protected List<String> extractSentencesFromParagraph(final String paragraph) {
    final Locale paragraphLanguage = this.detectLanguage(paragraph);
    if (paragraphLanguage == null
        || !this.isValidParagraph(paragraph, paragraphLanguage)) {
      return Collections.emptyList();
    }
    return this.extractSentencesFromParagraph(paragraph, paragraphLanguage);
  }

  protected List<String> extractSentencesFromParagraph(
      final String paragraph, final Locale paragraphLanguage) {
    // they are not threadsafe, so we create a new one each time
    final BreakIterator segmenter =
        BreakIterator.getSentenceInstance(paragraphLanguage);

    final List<String> sentences = new ArrayList<String>();
    for (final String sentence : this.getSegments(paragraph, segmenter)) {
      if (!this.isValidSentence(sentence, paragraphLanguage)) {
        sentences.add(sentence);
      }
    }
    return sentences;
  }
  
  protected boolean isValidParagraph(
      final String paragraph, final Locale paragraphLanguage) {
    return true;
  }
  
  protected boolean isValidSentence(
      final String sentence, final Locale paragraphLanguage) {
    return true;
  }
  
  protected Locale detectLanguage(final String text) {
    final Locale detectedLanguage = this.languageDetector.apply(text);
    if (!this.isTargetLanguage(detectedLanguage)) { return null; }
    return detectedLanguage;
  }

  protected boolean isTargetLanguage(final Locale language) {
    if (language == null) { return false; }

    return this.targetLanguages == null
        || this.targetLanguages.contains(language);
  }

  protected String normalizeWhitespace(final String text) {
    return text.replaceAll("\\s+", " ").trim();
  }
  
  protected List<String> getSegments(
      final String text, final BreakIterator segmenter) {
    segmenter.setText(text);

    final List<String> segments = new ArrayList<>();
    int begin = segmenter.first();
    int end = segmenter.next();
    while (end != BreakIterator.DONE) {
      segments.add(text.substring(begin, end));
      begin = end;
      end = segmenter.next();
    }

    return segments;
  }
  
  public void configure(final String prefix, final CommandLine config) {
    final String[] targetLanguages =
        config.getOptionValues(prefix + FLAG_EXTRACT_LANGUAGES);
    final boolean detectAll =
        config.hasOption(prefix + FLAG_EXTRACT_ALL_LANGUAGES);
    final String useLanguage =
        config.getOptionValue(prefix + FLAG_USE_LANGUAGE);
    final String paragraphSeparator =
        config.getOptionValue(prefix + FLAG_PARAGRAPH_SEPARATOR);
    final boolean doNotSeparateParagraphs =
        config.hasOption(prefix + FLAG_DO_NOT_SEPARATE_PARAGRAPHS);
    
    if (detectAll) {
      if (targetLanguages != null) {
        throw new IllegalStateException("Conflicting options: "
            + prefix + FLAG_EXTRACT_ALL_LANGUAGES + " and "
            + prefix + FLAG_EXTRACT_LANGUAGES);
      }
      if (useLanguage != null) {
        throw new IllegalStateException("Conflicting options: "
            + prefix + FLAG_EXTRACT_ALL_LANGUAGES + " and "
            + prefix + FLAG_USE_LANGUAGE);
      }
      this.setExtractAllLanguages();
    } else if (targetLanguages != null) {
      if (useLanguage != null) {
        throw new IllegalStateException("Conflicting options: "
            + prefix + FLAG_EXTRACT_LANGUAGES + " and "
            + prefix + FLAG_USE_LANGUAGE);
      }
      this.setExtractLanguages(targetLanguages);
    } else if (useLanguage != null) {
      this.setUseLanguage(useLanguage);
    }
    
    if (doNotSeparateParagraphs) {
      if (paragraphSeparator != null) {
        throw new IllegalStateException("Conflicting options: "
            + prefix + FLAG_PARAGRAPH_SEPARATOR + " and "
            + prefix + FLAG_DO_NOT_SEPARATE_PARAGRAPHS);
      }
      this.setDoNotSeparateParagraphs();
    } else if (paragraphSeparator != null) {
      this.setParagraphSeparator(paragraphSeparator);
    }
  }
  
  protected static Options addOptions(
      final Options options, final String prefix) {
    final Option targetLanguagesOption =
        new Option(prefix + FLAG_EXTRACT_LANGUAGES, true, "");
    targetLanguagesOption.setValueSeparator(',');
    options.addOption(targetLanguagesOption);
    options.addOption(prefix + FLAG_EXTRACT_ALL_LANGUAGES, false, "");
    options.addOption(prefix + FLAG_USE_LANGUAGE, true, "");
    options.addOption(prefix + FLAG_PARAGRAPH_SEPARATOR, true, "");
    options.addOption(prefix + FLAG_DO_NOT_SEPARATE_PARAGRAPHS, true, "");
    return options;
  }
  
  protected static Options addIOOptions(final Options options) {
    final Option inputOption = new Option(FLAG_INPUT, "");
    inputOption.setArgs(Option.UNLIMITED_VALUES);
    inputOption.setRequired(true);
    
    final Option outputOption = new Option(FLAG_OUTPUT, true, "");
    outputOption.setRequired(true);
    options.addOption(outputOption);
    return options;
  }
  
  public static void main(final String[] args)
  throws ParseException, IOException {
    final String noPrefix = "";

    final CommandLineParser parser = new GnuParser();
    final Options options =
        JerichoHtmlSentenceExtractor.addIOOptions(
            JerichoHtmlSentenceExtractor.addOptions(new Options(), noPrefix));
    final CommandLine config = parser.parse(options, args);

    final JerichoHtmlSentenceExtractor extractor =
        new JerichoHtmlSentenceExtractor();
    extractor.configure(noPrefix, config);
  }

}
