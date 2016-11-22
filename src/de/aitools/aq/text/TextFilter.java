package de.aitools.aq.text;

import java.util.List;
import java.util.Locale;

/**
 * 
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date$
 *
 */
public class TextFilter {
  
  private WordFilter wordFilter;
  
  private double minRatio;
  
  private int minAbsolute;
  
  public TextFilter(final WordFilter wordFilter) {
    this.setWordFilter(wordFilter);
    this.setMinRatio(0.0);
    this.setMinAbsolute(0);
  }
  
  public WordFilter getWordFilter() {
    return this.wordFilter;
  }
  
  public double getMinRatio() {
    return this.minRatio;
  }
  
  public int getMinAbsolute() {
    return this.minAbsolute;
  }
  
  public void setWordFilter(final WordFilter wordFilter) {
    if (wordFilter == null) { throw new NullPointerException(); }
    this.wordFilter = wordFilter;
  }
  
  public void setMinRatio(final double minRatio) {
    if (minRatio < 0.0) {
      throw new IllegalArgumentException("Negative ratio: " + minRatio);
    }
    this.minRatio = minRatio;
  }
  
  public void setMinAbsolute(final int minAbsolute) {
    if (minAbsolute < 0) {
      throw new IllegalArgumentException("Negative threshold: " + minAbsolute);
    }
    this.minAbsolute = minAbsolute;
  }

  public boolean test(final String text, final Locale language) {
    final List<String> words = WordFilter.toWords(text, language);
    return this.test(words, language);
  }

  public boolean test(final List<String> words, final Locale language) {
    final int numWords = words.size();
    final int numRemaining = this.wordFilter.filterWords(words, language).size();
    
    final double ratio = ((double) numRemaining) / ((double) numWords);
    return numRemaining >= this.minAbsolute && ratio >= this.minRatio;
  }

}
