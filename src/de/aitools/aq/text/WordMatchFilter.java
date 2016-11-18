package de.aitools.aq.text;

import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class WordMatchFilter extends WordFilter {
  
  private PatternPredicate predicate;
  
  public WordMatchFilter(final String pattern) {
    this.setPattern(pattern);
  }
  
  public WordMatchFilter(final Pattern pattern) {
    this.setPattern(pattern);
  }
  
  public Pattern getPattern() {
    return this.predicate.pattern;
  }
  
  public void setPattern(final String pattern) {
    this.setPattern(Pattern.compile(pattern));
  }
  
  public void setPattern(final Pattern pattern) {
    this.predicate = new PatternPredicate(pattern);
  }

  @Override
  public Predicate<String> getPredicate(final Locale language) {
    return this.predicate;
  }
  
  protected class PatternPredicate implements Predicate<String> {
    
    private final Pattern pattern;
    
    protected PatternPredicate(final Pattern pattern) {
      if (pattern == null) { throw new NullPointerException(); }
      this.pattern = pattern;
    }

    @Override
    public boolean test(final String word) {
      return this.pattern.matcher(word).matches();
    }
    
  }

}
