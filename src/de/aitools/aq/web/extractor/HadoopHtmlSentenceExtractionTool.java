package de.aitools.aq.web.extractor;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.cmu.lemurproject.WarcFileInputFormat;
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
      final Class<? extends HtmlSentenceExtractor> extractorClass) {
    configuration.set(PARAM_EXTRACTOR, extractorClass.getName());
  }

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public int run(final String[] args) throws Exception {
    final Configuration configuration = this.getConf();
    configuration.setStrings(PARAM_ARGS, args);

    @SuppressWarnings("unchecked")
    final Class<? extends HtmlSentenceExtractor> extractorClass =
        (Class<? extends HtmlSentenceExtractor>)
          Class.forName(configuration.get(PARAM_EXTRACTOR));
    final HtmlSentenceExtractor extractor = extractorClass.newInstance();
    final Job job = Job.getInstance(configuration, extractorClass.getName());
    
    final Options options = extractor.getOptions();
    final CommandLineParser parser = new GnuParser();
    final CommandLine config = parser.parse(options, args);
    
    job.setJobName(extractorClass.getName() + " " + Arrays.toString(args));
    job.setJarByClass(extractorClass);
    job.setMapperClass(WarcMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(WarcFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    TextOutputFormat.setCompressOutput(job, true);
    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    

    final String[] inputFileNames =
        config.getOptionValues(HtmlSentenceExtractor.FLAG_INPUT);
    for (final String inputFileName : inputFileNames) {
      FileInputFormat.addInputPath(job, new Path(inputFileName));
    }
    
    final String outputFileName =
        config.getOptionValue(HtmlSentenceExtractor.FLAG_OUTPUT);
    FileOutputFormat.setOutputPath(job, new Path(outputFileName));

    // Run it
    return job.waitForCompletion(true) ? 0 : 1;
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
