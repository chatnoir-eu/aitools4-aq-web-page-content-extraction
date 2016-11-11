package de.aitools.aq.web.extractor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import de.aitools.aq.text.StopWordFilter;
import de.aitools.aq.text.TextFilter;
import de.aitools.aq.text.WordFilter;
import de.aitools.aq.text.WordMatchFilter;

public class PotthastJerichoExtractor extends JerichoHtmlSentenceExtractor {

  // Default values implement the setting used in
  // "A Large-scale Analysis of the Mnemonic Password Advice"

  private static final String DEFAULT_WORD_PATTERN =
      "^\\p{IsAlphabetic}[-\\p{IsAlphabetic}]*\\p{IsAlphabetic}*$";

  public static final int DEFAULT_MIN_PARAGRAPH_LENGTH = 400;

  public static final int DEFAULT_MIN_NUM_STOP_WORDS_IN_SENTENCE = 1;

  public static final double DEFAULT_MIN_WORD_TO_TOKEN_RATIO = 0.5;
  
  private int minParagraphLengthInCharacters;
  
  private StopWordFilter stopWordFilter;
  
  private WordMatchFilter wordMatchFilter;

  private final TextFilter stopWordTextFilter;
  
  private final TextFilter wordMatchTextFilter;

  public PotthastJerichoExtractor() {
    this.setMinParagraphLengthInCharacters(DEFAULT_MIN_PARAGRAPH_LENGTH);
    this.stopWordFilter = new StopWordFilter(true);
    this.wordMatchFilter = new WordMatchFilter(DEFAULT_WORD_PATTERN);
    this.stopWordTextFilter = new TextFilter(this.stopWordFilter);
    this.setMinStopWordsInSentence(DEFAULT_MIN_NUM_STOP_WORDS_IN_SENTENCE);
    this.wordMatchTextFilter = new TextFilter(this.wordMatchFilter);
    this.setMinMatchingWordRatioInSentence(DEFAULT_MIN_WORD_TO_TOKEN_RATIO);
    this.setTargetLanguage(Locale.ENGLISH);
  }
  
  @Override
  public void setTargetLanguages(final Set<Locale> targetLanguages) {
    super.setTargetLanguages(targetLanguages);
    this.stopWordFilter.retainStopWordLists(targetLanguages);
  }
  
  public void setMinParagraphLengthInCharacters(
      final int minParagraphLengthInCharacters) {
    this.minParagraphLengthInCharacters = minParagraphLengthInCharacters;
  }
  
  public void setMinStopWordsInSentence(
      final int minStopWordsInSentence) {
    this.stopWordTextFilter.setMinAbsolute(minStopWordsInSentence);
  }
  
  public void setMinStopWordRatioInSentence(
      final double minStopWordRatioInSentence) {
    this.stopWordTextFilter.setMinRatio(minStopWordRatioInSentence);
  }
  
  public void setMinMatchingWordsInSentence(
      final int minMatchingWordsInSentence) {
    this.wordMatchTextFilter.setMinAbsolute(minMatchingWordsInSentence);
  }
  
  public void setMinMatchingWordRatioInSentence(
      final double minMatchingWordRatioInSentence) {
    this.wordMatchTextFilter.setMinRatio(minMatchingWordRatioInSentence);
  }
  
  public void setStopWordFilter(final StopWordFilter stopWordFilter) {
    this.stopWordTextFilter.setWordFilter(stopWordFilter);
    this.stopWordFilter = stopWordFilter;
  }
  
  public void setMatchingWordFilter(final WordMatchFilter wordMatchFilter) {
    this.wordMatchTextFilter.setWordFilter(wordMatchFilter);
    this.wordMatchFilter = wordMatchFilter;
  }
  
  @Override
  protected boolean isValidParagraph(
      final String paragraph, final Locale paragraphLanguage) {
    return paragraph.length() >= this.minParagraphLengthInCharacters;
  }
  
  @Override
  protected boolean isValidSentence(
      final String sentence, final Locale paragraphLanguage) {
    final List<String> words = WordFilter.toWords(sentence, paragraphLanguage);
    return this.stopWordTextFilter.test(words, paragraphLanguage)
        && this.wordMatchTextFilter.test(words, paragraphLanguage);
  }

  public static void main(final String[] args) throws IOException {
    final File inputFile = new File(args[0]);
    final PotthastJerichoExtractor extractor = new PotthastJerichoExtractor();

    final String html = FileUtils.readFileToString(inputFile);
    for (final String sentence : extractor.extractSentences(html)) {
      System.out.println(sentence);
    }
  }

}
