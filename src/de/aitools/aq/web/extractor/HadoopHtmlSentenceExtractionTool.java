package de.aitools.aq.web.extractor;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import edu.cmu.lemurproject.WritableWarcRecord;

public class HadoopHtmlSentenceExtractionTool implements Tool {

  //////////////////////////////////////////////////////////////////////////////
  //                                  CONSTANTS                               //
  //////////////////////////////////////////////////////////////////////////////
  
  private static final String PARAM_ARGS = "extraction.args";
  
  private static final String PARAM_EXTRACTOR = "extraction.extractor";

  //////////////////////////////////////////////////////////////////////////////
  //                                   MEMBERS                                //
  //////////////////////////////////////////////////////////////////////////////
  
  private Configuration configuration;

  //////////////////////////////////////////////////////////////////////////////
  //                                CONSTRUCTORS                              //
  //////////////////////////////////////////////////////////////////////////////
  
  public HadoopHtmlSentenceExtractionTool() {
    this.configuration = null;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                CONFIGURATION                             //
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public Configuration getConf() {
    return this.configuration;
  }

  @Override
  public void setConf(final Configuration configuration) {
    this.configuration = configuration;
  }
  
  public static void configure(
      final Configuration configuration,
      final Class<? extends HtmlSentenceExtractor> extractorClass,
      final String[] args) {
    configuration.set(PARAM_EXTRACTOR, extractorClass.getName());
    configuration.setStrings(PARAM_ARGS, args);
  }

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public int run(final String[] args) throws Exception {
    final Configuration configuration = this.getConf();
    @SuppressWarnings("unchecked")
    final Class<? extends HtmlSentenceExtractor> extractorClass =
        (Class<? extends HtmlSentenceExtractor>)
          Class.forName(configuration.get(PARAM_EXTRACTOR));
    final HtmlSentenceExtractor extractor = extractorClass.newInstance();
    final JobConf job = new JobConf(configuration, extractorClass);
    
    final Options options = extractor.getOptions();
    final CommandLineParser parser = new GnuParser();
    final CommandLine config = parser.parse(options, args);
    
    final String inputFileName =
        config.getOptionValue(HtmlSentenceExtractor.FLAG_INPUT);
    job.setJobName(extractorClass.getName() + " " + Arrays.toString(args));
    

    return 0;
  }

  public static class WarcMapper
  extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {

    public static enum COUNTERS {
      VALID_FILES,
      VALID_ZERO_SENTENCE_FILES,
      EXTRACTION_ERRORS,
      EXTRACTION_TIMEOUT_ERRORS,
      OUTPUT_NUM_SENTENCES,
    }
    
  }
  

}
