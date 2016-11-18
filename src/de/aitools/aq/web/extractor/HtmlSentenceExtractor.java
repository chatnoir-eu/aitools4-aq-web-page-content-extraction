package de.aitools.aq.web.extractor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
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

/**
 * Abstract base class for methods to extract sentences from HTML documents.
 * 
 * <p>
 * This class brings a timeout functionality and an extensible command line
 * interface to be used for both local and Hadoop jobs (using the 
 * {@link HadoopHtmlSentenceExtractionTool} class). HTML with extraction errors
 * is ignored (but an error message is sent to standard error when running
 * locally and the number of failed extractions is counted when running on
 * Hadoop).
 * </p><p>
 * Currently, it only supports reading HTML files when running locally and only
 * WARC files when running on Hadoop. Every class that extends this class will
 * automatically inherit this functionality.
 * </p><p>
 * When you extend this class, and your extractor is not configurable, you only
 * have to implement the {@link #extract(String)} method and add the following
 * main method:
 * <pre>
 * public static void main(final String[] args) throws Exception {
 *   HtmlSentenceExtractor.main(args, MySentenceExtractor.class);
 * }
 * </pre>
 * Where you replace <tt>MySentenceExtractor</tt> with the name of your class.
 * </p><p>
 * However, your extractor <b>must</b> have a constructor without arguments.
 * This constructor will also be used before command line argument parsing, so
 * it should not load resources unless it is absolutely needed.
 * </p><p>
 * When you want to add command line options for your extractor, you have to
 * override the {@link #addOptions(Options)} and {@link #configure(CommandLine)}
 * methods. In both cases, you <b>must</b> call the overridden method as first
 * statement in the method body (e.g., <tt>super.addOptions(options);</tt>).
 * For parsing command line arguments, <tt>addOptions</tt> is called. If parsing
 * was successful, <tt>configure</tt> will be called with the parse result, so
 * you can access the values of the options you defined in <tt>addOptions</tt>
 * in <tt>configure</tt>. 
 * </p><p>
 * See {@link JerichoHtmlSentenceExtractor} for a simple example of a sentence
 * extractor.
 * </p>
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date$
 *
 */
public abstract class HtmlSentenceExtractor {

  //////////////////////////////////////////////////////////////////////////////
  //                                  CONSTANTS                               //
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Value to use in {@link #setTimeoutInSeconds(int)} to specify that the
   * extractor should not timeout extraction attempts.
   */
  public static final int NO_TIMEOUT = -1;
  

  
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
  
  /**
   * Create a new extractor that does not timeout extraction attempts.
   */
  public HtmlSentenceExtractor() {
    this.executor = Executors.newCachedThreadPool();
    this.setNoTimeout();
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                   GETTERS                                //
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Checks whether this extractor will timeout extraction attempts.
   * @see #getTimeoutInSeconds()
   * @see #setNoTimeout()
   * @see #setTimeoutInSeconds(int)
   */
  public boolean hasTimeout() {
    return this.timeoutInSeconds != NO_TIMEOUT;
  }

  /**
   * Gets the number of seconds after which this extractor will timeout
   * extraction attempts, or {@link #NO_TIMEOUT} if it will never timeout.
   * @see #hasTimeout()
   * @see #setNoTimeout()
   * @see #setTimeoutInSeconds(int)
   */
  public int getTimeoutInSeconds() {
    return this.timeoutInSeconds;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                CONFIGURATION                             //
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Configures this extractor based on command line arguments.
   * <p>
   * The given <tt>CommandLine</tt> <b>must</b> be one created from an
   * {@link Options} object either obtained by calling {@link #getOptions()} or
   * modified by calling {@link #addOptions(Options)} on this extractor.
   * </p>
   * @param config The parsed configuration
   */
  public void configure(final CommandLine config) {
    final String timeout = config.getOptionValue(FLAG_TIMEOUT);
    if (timeout != null) {
      this.setTimeoutInSeconds(Integer.parseInt(timeout));
    }
  }
  
  /**
   * Configures this extractor to not timeout extraction attempts.
   * @see #setTimeoutInSeconds(int)
   */
  public void setNoTimeout() {
    this.setTimeoutInSeconds(NO_TIMEOUT);
  }
  
  /**
   * Configures this extractor to timeout extraction attempts.
   * @param timeoutInSeconds The number of seconds after which to timeout the
   * extraction attempt, then throwing an exception, or {@link #NO_TIMEOUT} to
   * not timeout attempts ever
   * @see #setNoTimeout()
   */
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

  /**
   * Extracts sentences from given HTML.
   * <p>
   * This method does not implement the timeout functionality, but will be
   * called by {@link #extractSentences(String)}, which does.
   * </p>
   * @param htmlInput The HTML input to extract sentences from
   * @return The extracted sentences
   * @throws IllegalArgumentException If the HTML can not be used for some
   * reason
   */
  protected abstract List<String> extract(final String htmlInput)
  throws IllegalArgumentException;
  
  /**
   * Extracts sentences from given HTML.
   * @param htmlInput The HTML to extract sentences from
   * @return The extracted sentences
   * @throws NullPointerException If the HTML is <tt>null</tt>
   * @throws ExecutionException If the extraction failed. When it fails due to a
   * timeout (see {@link #setTimeoutInSeconds(int)}), the exception will have a
   * {@link TimeoutException} as its cause
   */
  public List<String> extractSentences(final String htmlInput)
  throws NullPointerException, ExecutionException {
    if (htmlInput == null) { throw new NullPointerException(); }
    final HtmlSentenceExtractor extractor = this;
    
    if (this.timeoutInSeconds == NO_TIMEOUT) {
      return extractor.extract(htmlInput);
    } else {
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
  }
  
  //////////////////////////////////////////////////////////////////////////////
  //                                   PROGRAM                                //
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Gets all the {@link Options} of this extractor.
   * <p>
   * A new {@link Options} object is created on every call.
   * </p>
   */
  public final Options getOptions() {
    return this.addOptions(new Options());
  }

  /**
   * Add the options of this extractor to the given {@link Options}.
   * @param options The options to add to
   * @return The {@link Options} object that was provided as parameter, and that
   * now has the options of this extractor added to it
   */
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
        "Sets the timeout in seconds to try per HTML (Current: none)");
    timeoutOption.setLongOpt(FLAG_TIMEOUT);
    timeoutOption.setArgName("sec");
    options.addOption(timeoutOption);

    final Option numThreadsOption = new Option(SHORT_FLAG_NUM_THREADS, true,
        "Sets the number of web pages to extract in parallel (only used for "
        + MODE_LOCAL + " mode; Current: 1)");
    numThreadsOption.setLongOpt(FLAG_NUM_THREADS);
    numThreadsOption.setArgName("num");
    options.addOption(numThreadsOption);

    final Option writeFileNamesOption = new Option(SHORT_FLAG_WRITE_NAMES,
        "Configures this extractor to separate the sentences from different "
        + "pages in " + MODE_HADOOP + " by two empty lines and adds a line "
        + "containing the URI (and TREC-ID, if it exists) before the first "
        + "extracted sentence");
    writeFileNamesOption.setLongOpt(FLAG_WRITE_NAMES);
    options.addOption(writeFileNamesOption);
    
    return options;
  }
  
  /**
   * Runs an extractor based on command line arguments.
   * <p>
   * This includes selecting the mode (local or hadoop), option parsing (using
   * {@link #addOptions(Options)}), and configuration (using
   * {@link #configure(CommandLine)}). 
   * </p>
   * @param args The command line arguments
   * @param extractorClass The class of the sentence extractor (used to create
   * a new instance of it via the default constructor, which has no arguments)
   * @throws Exception If something goes wrong
   */
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
  
  private static void extractLocal(
      final HtmlSentenceExtractor extractor,
      final CommandLine config)
  throws InterruptedException, InstantiationException,
  IllegalAccessException {
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
            } catch (final ExecutionException e) {
              // Continue with next
              System.err.println("EXTRACTION ERROR on parsing " + inputFile
                  + ": " + e.getMessage());
            } catch (final IOException e) {
              throw new UncheckedIOException(e);
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
  
  private static void printHelp(
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
