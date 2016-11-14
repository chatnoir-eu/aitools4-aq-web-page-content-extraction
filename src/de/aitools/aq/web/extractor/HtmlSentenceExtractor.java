package de.aitools.aq.web.extractor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import edu.cmu.lemurproject.WarcRecord;

public abstract class HtmlSentenceExtractor {

  //////////////////////////////////////////////////////////////////////////////
  //                                  CONSTANTS                               //
  //////////////////////////////////////////////////////////////////////////////
  
  protected static final Pattern HTML_CONTENT_TYPE_PATTERN = Pattern.compile(
      "text/html.*");

  protected static final String HEADER_START = "HTTP/";

  protected static final String HEADER_CONTENT_TYPE = "Content-Type:";

  protected static final Pattern HEADER_END_PATTERN = Pattern.compile(
      "\r?\n\r?\n");
  

  
  private static final String MODE_LOCAL = "local";
  
  private static final String MODE_HADOOP = "hadoop";
  
  private static final String[] MODES = { MODE_LOCAL, MODE_HADOOP };



  private static String SHORT_FLAG_INPUT = "i";

  public static String FLAG_INPUT = "input-files";

  private static String SHORT_FLAG_OUTPUT = "o";
  
  public static String FLAG_OUTPUT = "output-directory";

  private static String SHORT_FLAG_HELP = "h";
  
  protected static String FLAG_HELP = "help";

  private static String SHORT_FLAG_NUM_THREADS = "t";
  
  private static String FLAG_NUM_THREADS = "num-threads";

  //////////////////////////////////////////////////////////////////////////////
  //                                   MEMBERS                                //
  //////////////////////////////////////////////////////////////////////////////
  
  private final ExecutorService executor;

  //////////////////////////////////////////////////////////////////////////////
  //                                CONSTRUCTORS                              //
  //////////////////////////////////////////////////////////////////////////////
  
  public HtmlSentenceExtractor() {
    this.executor = Executors.newCachedThreadPool();
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                CONFIGURATION                             //
  //////////////////////////////////////////////////////////////////////////////

  public abstract void configure(final CommandLine config);

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////

  public List<String> extractSentences(final WarcRecord record)
  throws IOException {
    if (!record.getHeaderRecordType().equals("response")) { return null; }

    final String warcContent = record.getContentUTF8().trim();

    final String httpHeader = this.getHeader(warcContent);
    if (httpHeader == null) { return null; }

    final String contentType = this.getContentType(httpHeader);
    if (contentType == null) { return null; }

    if (!HTML_CONTENT_TYPE_PATTERN.matcher(contentType).matches()) {
      return null;
    }
    
    final String httpContent = warcContent.substring(httpHeader.length());
    return this.extractSentences(httpContent);
  }

  public abstract List<String> extractSentences(final String htmlInput)
  throws NullPointerException, IllegalArgumentException;

  protected String getContentType(final String httpHeader) {
    final int fieldStart = httpHeader.indexOf(HEADER_CONTENT_TYPE);
    if (fieldStart < 0) { return null; }

    int fieldEnd = httpHeader.indexOf("\n", fieldStart);
    if (fieldEnd < 0) {
      fieldEnd = httpHeader.length();
    }

    final String fieldValue = httpHeader.substring(
        fieldStart + HEADER_CONTENT_TYPE.length(),
        fieldEnd);

    return fieldValue.trim().toLowerCase();
  }
  
  public List<String> extractSentences(
      final WarcRecord warcRecord, final int timeoutInSeconds)
  throws InterruptedException, ExecutionException, TimeoutException {
    final HtmlSentenceExtractor extractor = this;

    final Callable<List<String>> task = new Callable<List<String>>() {
       public List<String> call() throws IOException {
          return extractor.extractSentences(warcRecord);
       }
    };
    final Future<List<String>> future = this.executor.submit(task);
    return future.get(timeoutInSeconds, TimeUnit.SECONDS);
  }
  
  public List<String> extractSentences(
      final String htmlInput, final int timeoutInSeconds)
  throws InterruptedException, ExecutionException, TimeoutException {
    final HtmlSentenceExtractor extractor = this;

    final Callable<List<String>> task = new Callable<List<String>>() {
       public List<String> call() throws IOException {
          return extractor.extractSentences(htmlInput);
       }
    };
    final Future<List<String>> future = this.executor.submit(task);
    return future.get(timeoutInSeconds, TimeUnit.SECONDS);
  }

  protected String getHeader(final String warcContent) {
    if (!warcContent.startsWith(HEADER_START)) { return null; }

    final Matcher endMatcher = HEADER_END_PATTERN.matcher(warcContent);
    if (!endMatcher.find()) {
      return null;
    }
    final int httpHeaderEnd = endMatcher.end();

    return warcContent.substring(0, httpHeaderEnd);
  }
  
  //////////////////////////////////////////////////////////////////////////////
  //                                   PROGRAM                                //
  //////////////////////////////////////////////////////////////////////////////
  
  protected static void main(final String[] args,
      final Class<? extends HtmlSentenceExtractor> extractorClass)
  throws Exception {
    final HtmlSentenceExtractor extractor = extractorClass.newInstance();
    final Options options = extractor.getOptions();
    if (args.length == 0) {
      HtmlSentenceExtractor.printHelp(extractorClass, options, 1);
    }
    final String mode = args[0];

    final CommandLineParser parser = new GnuParser();
    try {
      final CommandLine config = parser.parse(options, args);
      if (config.hasOption(FLAG_HELP)) {
        HtmlSentenceExtractor.printHelp(extractorClass, options, 0);
      }

      switch (mode) {
      case MODE_LOCAL:
        HtmlSentenceExtractor.extractLocal(extractor, config);
        System.exit(0);
        break;
        
      case MODE_HADOOP:
        final Configuration hadoopConfig = new Configuration();
        HadoopHtmlSentenceExtractionTool.configure(
            hadoopConfig, extractorClass);
        System.exit(ToolRunner.run(
            hadoopConfig, new HadoopHtmlSentenceExtractionTool(), args));
        break;

      default:
        System.err.println("Unknown mode: " + mode);
        HtmlSentenceExtractor.printHelp(extractorClass, options, 1);
        break;
      }
    } catch (final AlreadySelectedException | MissingOptionException
        | UnrecognizedOptionException e) {
      System.err.println(e.getMessage());
      HtmlSentenceExtractor.printHelp(extractorClass, options, 1);
    }
  }

  public Options getOptions() {
    return this.addOptions(new Options());
  }

  public Options addOptions(final Options options) {
    final Option inputOption = new Option(SHORT_FLAG_INPUT,
        "Sets the input files to extract the sentences from");
    inputOption.setLongOpt(FLAG_INPUT);
    inputOption.setArgName("file,file,...");
    inputOption.setArgs(Option.UNLIMITED_VALUES);
    inputOption.setRequired(true);
    inputOption.setValueSeparator(',');
    options.addOption(inputOption);
    
    final Option outputOption = new Option(SHORT_FLAG_OUTPUT, true,
        "Sets the directory to which extracted sentences are written");
    outputOption.setLongOpt(FLAG_OUTPUT);
    outputOption.setArgName("dir");
    outputOption.setRequired(true);
    options.addOption(outputOption);

    final Option helpOption = new Option(SHORT_FLAG_HELP,
        "Prints this message");
    helpOption.setLongOpt(FLAG_HELP);
    options.addOption(helpOption);

    final Option numThreadsOption = new Option(SHORT_FLAG_NUM_THREADS, true,
        "Sets the number of web pages to extract in parallel (only used for "
            + MODE_LOCAL + " mode)");
    numThreadsOption.setLongOpt(FLAG_NUM_THREADS);
    numThreadsOption.setArgName("num");
    options.addOption(numThreadsOption);
    
    return options;
  }
  
  protected static void extractLocal(
      final HtmlSentenceExtractor extractor,
      final CommandLine config)
  throws InterruptedException, InstantiationException, IllegalAccessException {
    extractor.configure(config);

    final int numThreads =
        Integer.parseInt(config.getOptionValue(FLAG_NUM_THREADS, "1"));
    final Queue<File> inputFiles = new ConcurrentLinkedQueue<File>();
    for (final String inputFileName : config.getOptionValues(FLAG_INPUT)) {
      inputFiles.add(new File(inputFileName));
    }
    final File outputDirectory = new File(config.getOptionValue(FLAG_OUTPUT));
    outputDirectory.mkdirs();
    

    final Thread[] threads = new Thread[numThreads];
    for (int t = 0; t < numThreads; ++t) {
      threads[t] = new Thread() {
        @Override
        public void run() {
          for (File inputFile = inputFiles.poll();
              inputFile != null;
              inputFile = inputFiles.poll()) {
            System.err.println("Extracting " + inputFile);
            final File outputFile =
                new File(outputDirectory, inputFile.getName());
            try (final BufferedWriter writer =
                new BufferedWriter(new FileWriter(outputFile))) {
              for (final String sentence
                  : extractor.extractSentences(FileUtils.readFileToString(
                      inputFile))) {
                writer.append(sentence).append('\n');
              }
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
      threads[t].start();
    }
    
    for (final Thread thread : threads) {
      thread.join();
    }
  }
  
  protected static void printHelp(
      final Class<?> classType, final Options options, final int exitCode) {
    System.err.println();
    final HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(120);
    formatter.setOptionComparator(new Comparator<Option>() {
      @Override
      public int compare(final Option o1, final Option o2) {
        if (o1.isRequired() && !o2.isRequired()) {
          return -1;
        } else if (!o1.isRequired() && o2.isRequired()) {
          return 1;
        } else {
          return o1.getOpt().compareTo(o2.getOpt());
        }
      }
    });
    formatter.printHelp(classType.getName() + " [local|hadoop]", options, true);
    System.exit(exitCode);
  }

}
