package de.aitools.aq.web.extractor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
  
  public static final int NO_TIMEOUT = -1;
  
  protected static final Pattern HTML_CONTENT_TYPE_PATTERN = Pattern.compile(
      "text/html.*");

  protected static final String HEADER_START = "HTTP/";

  protected static final String HEADER_CONTENT_TYPE = "Content-Type:";

  protected static final Pattern HEADER_END_PATTERN = Pattern.compile(
      "\r?\n\r?\n");
  

  
  private static final String MODE_LOCAL = "local";
  
  private static final String MODE_HADOOP = "hadoop";



  public static String SHORT_FLAG_INPUT = "i";

  public static String FLAG_INPUT = "input";

  public static String SHORT_FLAG_OUTPUT = "o";
  
  public static String FLAG_OUTPUT = "output";

  public static String SHORT_FLAG_HELP = "h";
  
  public static String FLAG_HELP = "help";

  public static String SHORT_FLAG_NUM_THREADS = "t";
  
  public static String FLAG_NUM_THREADS = "threads";

  public static String SHORT_FLAG_TIMEOUT = "s";

  public static String FLAG_TIMEOUT = "timeout-in-seconds";

  public static String SHORT_FLAG_WRITE_NAMES = "n";

  public static String FLAG_WRITE_NAMES = "write-names";

  //////////////////////////////////////////////////////////////////////////////
  //                                   MEMBERS                                //
  //////////////////////////////////////////////////////////////////////////////
  
  private final ExecutorService executor;
  
  private int timeoutInSeconds;

  //////////////////////////////////////////////////////////////////////////////
  //                                CONSTRUCTORS                              //
  //////////////////////////////////////////////////////////////////////////////
  
  public HtmlSentenceExtractor() {
    this.executor = Executors.newCachedThreadPool();
    this.setNoTimeout();
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                   GETTERS                                //
  //////////////////////////////////////////////////////////////////////////////
  
  public boolean hasTimeout() {
    return this.timeoutInSeconds != NO_TIMEOUT;
  }
  
  public int getTimeoutInSeconds() {
    return this.timeoutInSeconds;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                CONFIGURATION                             //
  //////////////////////////////////////////////////////////////////////////////

  public void configure(final CommandLine config) {
    final String timeout = config.getOptionValue(FLAG_TIMEOUT);
    if (timeout != null) {
      this.setTimeoutInSeconds(Integer.parseInt(timeout));
    }
  }
  
  public void setNoTimeout() {
    this.setTimeoutInSeconds(NO_TIMEOUT);
  }
  
  public void setTimeoutInSeconds(final int timeoutInSeconds) {
    if (timeoutInSeconds <= 0 && timeoutInSeconds != NO_TIMEOUT) {
      throw new IllegalArgumentException(
          "Non-positive timeout: " + timeoutInSeconds);
    }
    this.timeoutInSeconds = timeoutInSeconds;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////

  protected abstract List<String> extract(final String htmlInput)
  throws NullPointerException, IllegalArgumentException;
  
  public List<String> extractSentences(final String htmlInput)
  throws ExecutionException {
    final HtmlSentenceExtractor extractor = this;

    final Callable<List<String>> task = new Callable<List<String>>() {
       public List<String> call() throws IOException {
          return extractor.extract(htmlInput);
       }
    };
    final Future<List<String>> future = this.executor.submit(task);
    try {
      return future.get(this.timeoutInSeconds, TimeUnit.SECONDS);
    } catch (final Throwable e) {
      future.cancel(true);
      throw new ExecutionException(e);
    }
  }
  
  public static String extractHtml(final WarcRecord record)
  throws IOException {
    if (!record.getHeaderRecordType().equals("response")) { return null; }

    final String warcContent = record.getContentUTF8().trim();

    final String httpHeader = HtmlSentenceExtractor.getHeader(warcContent);
    if (httpHeader == null) { return null; }

    final String contentType = HtmlSentenceExtractor.getContentType(httpHeader);
    if (contentType == null) { return null; }

    if (!HTML_CONTENT_TYPE_PATTERN.matcher(contentType).matches()) {
      return null;
    }
    
    return warcContent.substring(httpHeader.length());
  }

  protected static String getHeader(final String warcContent) {
    if (!warcContent.startsWith(HEADER_START)) { return null; }

    final Matcher endMatcher = HEADER_END_PATTERN.matcher(warcContent);
    if (!endMatcher.find()) {
      return null;
    }
    final int httpHeaderEnd = endMatcher.end();

    return warcContent.substring(0, httpHeaderEnd);
  }

  protected static String getContentType(final String httpHeader) {
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
    final String[] reducedArgs = new String[args.length - 1];
    System.arraycopy(args, 1, reducedArgs, 0, reducedArgs.length);

    final CommandLineParser parser = new GnuParser();
    try {
      final CommandLine config = parser.parse(options, reducedArgs);
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
            hadoopConfig, new HadoopHtmlSentenceExtractionTool(), reducedArgs));
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

    final Option timeoutOption = new Option(SHORT_FLAG_TIMEOUT, true,
        "Sets the timeout in seconds to try per HTML");
    timeoutOption.setLongOpt(FLAG_TIMEOUT);
    timeoutOption.setArgName("sec");
    options.addOption(timeoutOption);

    final Option numThreadsOption = new Option(SHORT_FLAG_NUM_THREADS, true,
        "Sets the number of web pages to extract in parallel (only used for "
        + MODE_LOCAL + " mode)");
    numThreadsOption.setLongOpt(FLAG_NUM_THREADS);
    numThreadsOption.setArgName("num");
    options.addOption(numThreadsOption);

    final Option writeFileNamesOption = new Option(SHORT_FLAG_WRITE_NAMES,
        "Separates the sentences from different pages in " + MODE_HADOOP
        + " by two empty lines and adds a line containing the URI (and "
        + "TREC-ID, if it exists) before the first extracted sentence");
    writeFileNamesOption.setLongOpt(FLAG_WRITE_NAMES);
    options.addOption(writeFileNamesOption);
    
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
    final String usage = classType.getName() + " local|hadoop "
        + "[hadoop-options] [options]";
    final String header = "The first argument must always be the mode (either "
        + "'local' or 'hadoop'). In hadoop mode, the options that apply to all "
        + "hadoop jobs can be specified directly after this. You still have to "
        + "use the hadoop command (instead of java) when using hadoop mode.";
    final String footer = "";
    formatter.printHelp(usage, header, options, footer, false);
    System.exit(exitCode);
  }

}
