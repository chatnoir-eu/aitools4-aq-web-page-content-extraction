package de.aitools.aq.text;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import de.aitools.ie.stopwords.StopWordList;

public class StopWordFilter extends WordFilter {
  
  protected static final Set<String> NO_LIST_AVAILABLE = null;

  private final Map<Locale, StopWordPredicate> stopWordLists;
  
  private final boolean ignoreCase;
  
  public StopWordFilter() {
    this(true);
  }
  
  public StopWordFilter(final boolean ignoreCase) {
    this.stopWordLists = new HashMap<>();
    this.ignoreCase = ignoreCase;
  }
  
  public boolean getIgnoresCase() {
    return this.ignoreCase;
  }

  public void addStopWords(
      final Locale language, final Iterable<String> words) {
    if (language == null) { throw new NullPointerException(); }
    if (words == null) { throw new NullPointerException(); }
    StopWordPredicate predicate = this.stopWordLists.get(language);
    if (predicate == null) {
      predicate = new StopWordPredicate(language);
      this.stopWordLists.put(language, predicate);
    }
    predicate.addStopWords(words);
  }

  public void addStopWords(
      final Locale language, final String[] words) {
    if (language == null) { throw new NullPointerException(); }
    if (words == null) { throw new NullPointerException(); }
    StopWordPredicate predicate = this.stopWordLists.get(language);
    if (predicate == null) {
      predicate = new StopWordPredicate(language);
      this.stopWordLists.put(language, predicate);
    }
    predicate.addStopWords(words);
  }
  
  public void retainStopWordLists(final Collection<Locale> languages) {
    this.stopWordLists.keySet().removeIf(
        language -> !languages.contains(language));
  }
  
  public void clearStopWordLists() {
    this.stopWordLists.clear();
  }
  
  public void removeStopWords(final Locale language) {
    this.stopWordLists.remove(language);
  }

  @Override
  public Predicate<String> getPredicate(final Locale language) {
    final Predicate<String> predicate = this.stopWordLists.get(language);
    if (predicate != null) {
      return predicate;
    } else if (this.stopWordLists.containsKey(language)) {
      throw new IllegalArgumentException("Language not supported: " + language);
    } else {
      synchronized (this.stopWordLists) {
        if (!this.stopWordLists.containsKey(language)) {
          try {
            final String[] stopWords =
                new StopWordList(language).getStopWordList();
            this.addStopWords(language, stopWords);
          } catch (final Error e) {
            this.stopWordLists.put(language, null);
          }
        }
      }
      return this.getPredicate(language);
    }
  }
  
  protected class StopWordPredicate implements Predicate<String> {
    
    private final Set<String> stopWords;
    
    private final Locale language;
    
    public StopWordPredicate(final Locale language) {
      if (language == null) { throw new NullPointerException(); }
      this.stopWords = new HashSet<>();
      this.language = language;
    }

    @Override
    public boolean test(final String word) {
      return this.stopWords.contains(this.normalize(word));
    }
    
    protected void addStopWords(final String[] words) {
      for (final String word : words) {
        this.stopWords.add(this.normalize(word));
      }
    }
    
    protected void addStopWords(final Iterable<String> words) {
      for (final String word : words) {
        this.stopWords.add(this.normalize(word));
      }
    }
    
    protected String normalize(final String word) {
      if (StopWordFilter.this.ignoreCase) {
        return word.toLowerCase(this.language);
      } else {
        return word;
      }
    }
    
  }

}
