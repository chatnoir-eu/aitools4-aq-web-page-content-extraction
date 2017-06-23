package de.aitools.aq.web.extractor;

import java.io.IOException;

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
import org.apache.hadoop.util.ToolRunner;

import edu.cmu.lemurproject.WarcFileInputFormat;
import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

/**
 * Class that extracts all URIs that have a TREC ID together with this ID.
 *
 * <p>
 * Currently, this only supports reading WARCs.
 * </p>
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date$
 *
 */
public class HadoopHtmlTrecUriExtractionTool implements Tool {

  //////////////////////////////////////////////////////////////////////////////
  //                                   MEMBERS                                //
  //////////////////////////////////////////////////////////////////////////////
  
  private Configuration configuration;

  //////////////////////////////////////////////////////////////////////////////
  //                                CONSTRUCTORS                              //
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Creates a new tool.
   */
  public HadoopHtmlTrecUriExtractionTool() {
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

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public int run(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage:");
      System.err.println("  <input1> [<input2> [...]] <output>");
      return 1;
    }

    final Job job = Job.getInstance(this.getConf(), this.getClass().getName());

    job.setJarByClass(this.getClass());
    job.setMapperClass(WarcMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(WarcFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    TextOutputFormat.setCompressOutput(job, true);
    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    

    for (int a = 0; a < (args.length - 1); ++a) {
      FileInputFormat.addInputPath(job, new Path(args[a]));
    }

    FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

    // Run it
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(new HadoopHtmlTrecUriExtractionTool(), args));
  }

  /**
   *
   * @author johannes.kiesel@uni-weimar.de
   * @version $Date$
   *
   */
  public static class WarcMapper
  extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {

    public static enum COUNTERS {
      VALID,
      INVALID_BY_ERROR,
      INVALID_NO_HTML,
      INVALID_NO_TREC_ID,
      INVALID_NO_LOCATION,
    }

    @Override
    protected void map(final LongWritable key, final WritableWarcRecord value,
        final Context context)
    throws IOException, InterruptedException {
      final WarcRecord warcRecord = value.getRecord();
      try {
        if (Warcs.isHtml(warcRecord)) {
          final String trecId =
              warcRecord.getHeaderMetadataItem("WARC-TREC-ID");
          if (trecId != null) {
            final String targetUri =
                warcRecord.getHeaderMetadataItem("WARC-Target-URI");
            if (targetUri != null) {
              final Text id = new Text(trecId);
              final Text uri = new Text(targetUri);
              context.write(id, uri);
              context.getCounter(COUNTERS.VALID).increment(1);
            } else {
              context.getCounter(COUNTERS.INVALID_NO_LOCATION).increment(1);
            }
          } else {
            context.getCounter(COUNTERS.INVALID_NO_TREC_ID).increment(1);
          }
        } else {
          context.getCounter(COUNTERS.INVALID_NO_HTML).increment(1);
        }
      } catch (final Throwable e) {
        context.getCounter(COUNTERS.INVALID_BY_ERROR).increment(1);
      }
      context.progress();
    }
    
  }
  

}
