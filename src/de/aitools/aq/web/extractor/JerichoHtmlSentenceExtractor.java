package de.aitools.aq.web.extractor;

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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import com.ibm.icu.text.BreakIterator;

import de.aitools.ie.languagedetection.LanguageDetector;
import net.htmlparser.jericho.Renderer;
import net.htmlparser.jericho.Segment;
import net.htmlparser.jericho.Source;

public class JerichoHtmlSentenceExtractor extends HtmlSentenceExtractor {

  //////////////////////////////////////////////////////////////////////////////
  //                                  CONSTANTS                               //
  //////////////////////////////////////////////////////////////////////////////
  
  private static String SHORT_FLAG_EXTRACT_ALL_LANGUAGES = "la";
  
  private static String FLAG_EXTRACT_ALL_LANGUAGES = "language-extract-all";
  
  private static String SHORT_FLAG_EXTRACT_LANGUAGES = "le";
  
  private static String FLAG_EXTRACT_LANGUAGES = "language-extract";
  
  private static String SHORT_FLAG_USE_LANGUAGE = "lu";
  
  private static String FLAG_USE_LANGUAGE = "language-use";
  
  private static String SHORT_FLAG_DO_NOT_SEPARATE_PARAGRAPHS = "pn";
  
  private static String FLAG_DO_NOT_SEPARATE_PARAGRAPHS = "separate-paragraphs-not";
  
  private static String SHORT_FLAG_PARAGRAPH_SEPARATOR = "pw";
  
  private static String FLAG_PARAGRAPH_SEPARATOR = "separate-paragraphs-with";

  //////////////////////////////////////////////////////////////////////////////
  //                                   MEMBERS                                //
  //////////////////////////////////////////////////////////////////////////////

  private Set<String> targetLanguages;

  private Function<String, Locale> languageDetector;
  
  private String paragraphSeparator;
  
  private boolean separateParagraphs;

  //////////////////////////////////////////////////////////////////////////////
  //                                CONSTRUCTORS                              //
  //////////////////////////////////////////////////////////////////////////////
  
  public JerichoHtmlSentenceExtractor() {
    this.setExtractLanguage(Locale.ENGLISH);
    this.setDoNotSeparateParagraphs();
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                   GETTER                                 //
  //////////////////////////////////////////////////////////////////////////////

  public Set<String> getTargetLanguages() {
    return Collections.unmodifiableSet(this.targetLanguages);
  }
  
  public Function<String, Locale> getLanguageDetector() {
    if (this.languageDetector == null) {
      final LanguageDetector languageDetector = new LanguageDetector();
      this.setLanguageDetector(text -> languageDetector.detect(text));
    }
    return this.languageDetector;
  }
  
  public String getParagraphSeparator() {
    return this.paragraphSeparator;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                CONFIGURATION                             //
  //////////////////////////////////////////////////////////////////////////////
  
  public void setExtractAllLanguages() {
    this.targetLanguages = null;
    this.languageDetector = null;
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
    this.targetLanguages = new HashSet<>(targetLanguages.size());
    for (final Locale targetLanguage : targetLanguages) {
      this.targetLanguages.add(targetLanguage.getLanguage());
    }
    this.languageDetector = null;
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
  public void configure(final CommandLine config) {
    final String[] targetLanguages =
        config.getOptionValues(FLAG_EXTRACT_LANGUAGES);
    final boolean detectAll =
        config.hasOption(FLAG_EXTRACT_ALL_LANGUAGES);
    final String useLanguage =
        config.getOptionValue(FLAG_USE_LANGUAGE);
    final String paragraphSeparator =
        config.getOptionValue(FLAG_PARAGRAPH_SEPARATOR);
    final boolean doNotSeparateParagraphs =
        config.hasOption(FLAG_DO_NOT_SEPARATE_PARAGRAPHS);
    
    if (detectAll) {
      this.setExtractAllLanguages();
    } else if (targetLanguages != null) {
      this.setExtractLanguages(targetLanguages);
    } else if (useLanguage != null) {
      this.setUseLanguage(useLanguage);
    }
    
    if (doNotSeparateParagraphs) {
      this.setDoNotSeparateParagraphs();
    } else if (paragraphSeparator != null) {
      this.setParagraphSeparator(paragraphSeparator);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////

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
    // they are not thread-safe, so we create a new one each time
    final BreakIterator segmenter =
        BreakIterator.getSentenceInstance(paragraphLanguage);

    final List<String> sentences = new ArrayList<String>();
    for (final String sentence : this.getSegments(paragraph, segmenter)) {
      if (!sentence.isEmpty()) {
        if (this.isValidSentence(sentence, paragraphLanguage)) {
          sentences.add(sentence);
        }
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
    final Locale detectedLanguage = this.getLanguageDetector().apply(text);
    if (!this.isTargetLanguage(detectedLanguage)) { return null; }
    return detectedLanguage;
  }

  protected boolean isTargetLanguage(final Locale language) {
    if (language == null) { return false; }

    return this.targetLanguages == null
        || this.targetLanguages.contains(language.getLanguage());
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
      segments.add(text.substring(begin, end).trim());
      begin = end;
      end = segmenter.next();
    }

    return segments;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                   PROGRAM                                //
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  public Options addOptions(final Options options) {
    super.addOptions(options);
    final OptionGroup languages = new OptionGroup();
    final Option targetLanguagesOption =
        new Option(SHORT_FLAG_EXTRACT_LANGUAGES, true,
            "");
    targetLanguagesOption.setLongOpt(FLAG_EXTRACT_LANGUAGES);
    targetLanguagesOption.setArgs(Option.UNLIMITED_VALUES);
    targetLanguagesOption.setValueSeparator(',');
    targetLanguagesOption.setArgName("lang,lang,...");
    languages.addOption(targetLanguagesOption);
    
    final Option allLanguagesOption =
        new Option(SHORT_FLAG_EXTRACT_ALL_LANGUAGES, false,
            "");
    allLanguagesOption.setLongOpt(FLAG_EXTRACT_ALL_LANGUAGES);
    languages.addOption(allLanguagesOption);
    
    final Option useLanguageOption = new Option(SHORT_FLAG_USE_LANGUAGE, true,
        "");
    useLanguageOption.setLongOpt(FLAG_USE_LANGUAGE);
    useLanguageOption.setArgName("lang");
    languages.addOption(useLanguageOption);
    options.addOptionGroup(languages);
    
    
    
    final OptionGroup paragraphs = new OptionGroup();
    final Option paragraphSeparatorOption = new Option(
        SHORT_FLAG_PARAGRAPH_SEPARATOR, true,
            "");
    paragraphSeparatorOption.setLongOpt(FLAG_PARAGRAPH_SEPARATOR);
    paragraphSeparatorOption.setArgName("sep");
    paragraphs.addOption(paragraphSeparatorOption);
    
    final Option paragraphNotSeparateOption = new Option(
        SHORT_FLAG_DO_NOT_SEPARATE_PARAGRAPHS, false,
        "");
    paragraphNotSeparateOption.setLongOpt(FLAG_DO_NOT_SEPARATE_PARAGRAPHS);
    paragraphs.addOption(paragraphNotSeparateOption);
    options.addOptionGroup(paragraphs);

    return options;
  }
  
  public static void main(final String[] args) throws Exception {
    HtmlSentenceExtractor.main(args, JerichoHtmlSentenceExtractor.class);
  }

}
