package net.opentsdb.tools;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.meta.Annotation;
import static net.opentsdb.tools.DumpSeries.appendAnnotation;
import static net.opentsdb.tools.DumpSeries.appendImportCell;
import static net.opentsdb.tools.DumpSeries.appendRawCell;
import static net.opentsdb.tools.DumpSeries.date;
import net.opentsdb.utils.Config;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.KeyValue;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Magic {

    private static final Logger LOG = LoggerFactory.getLogger(Magic.class);

    /**
     * Prints usage and exits with the given retval.
     */
    private static void usage(final ArgP argp, final String errmsg,
            final int retval) {
        System.err.println(errmsg);
        System.err.println("Usage: magic"
                + " START-DATE [END-DATE] query new_tag_value_pair\n");
        System.err.print(argp.usage());
        System.exit(retval);
    }

    public static void main(String[] args) throws Exception {
        ArgP argp = new ArgP();
        CliOptions.addCommon(argp);
        args = CliOptions.parse(argp, args);
        if (args == null) {
            usage(argp, "Invalid usage.", 1);
        } else if (args.length < 4) {
            usage(argp, "Not enough arguments.", 2);
        }

        // get a config object
        Config config = CliOptions.getConfig(argp);

        final TSDB tsdb = new TSDB(config);
        tsdb.checkNecessaryTablesExist().joinUninterruptibly();
        final byte[] table = config.getString("tsd.storage.hbase.data_table").getBytes();
        final boolean delete = true;
        final boolean importformat = true;
        argp = null;
        try {
            doMagic(tsdb, tsdb.getClient(), table, delete, importformat, args);
        } finally {
            tsdb.shutdown().joinUninterruptibly();
        }
    }

    private static void doMagic(final TSDB tsdb,
            final HBaseClient client,
            final byte[] table,
            final boolean delete,
            final boolean importformat,
            final String[] args) throws Exception {
        final ArrayList<Query> queries = new ArrayList<Query>();

        //take out last tag value pair since it should'n be in a query
        String newTagValuePair = args[args.length - 1];
        String[] queryArgs = Arrays.copyOf(args, args.length - 1);

        System.out.println("New tag value pair is: " + newTagValuePair);

        CliQuery.parseCommandLineQuery(queryArgs, tsdb, queries, null, null);

        final StringBuilder buf = new StringBuilder();
        for (final Query query : queries) {
            final List<org.hbase.async.Scanner> scanners = Internal.getScanners(query);
            for (org.hbase.async.Scanner scanner : scanners) {
                ArrayList<ArrayList<KeyValue>> rows;
                while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                    for (final ArrayList<KeyValue> row : rows) {
                        // where exported line will go
                        String exportedData = null;

                        buf.setLength(0);
                        final byte[] key = row.get(0).key();
                        final long base_time = Internal.baseTime(tsdb, key);
                        final String metric = Internal.metricName(tsdb, key);
                        // Print the row key.
                        if (!importformat) {
                            buf.append(Arrays.toString(key))
                                    .append(' ')
                                    .append(metric)
                                    .append(' ')
                                    .append(base_time)
                                    .append(" (").append(date(base_time)).append(") ");
                            try {
                                buf.append(Internal.getTags(tsdb, key));
                            } catch (RuntimeException e) {
                                buf.append(e.getClass().getName() + ": " + e.getMessage());
                            }
                            buf.append('\n');
                            System.out.print(buf);
                        }

                        // Print individual cells.
                        buf.setLength(0);
                        if (!importformat) {
                            buf.append("  ");
                        }
                        for (final KeyValue kv : row) {
                            // Discard everything or keep initial spaces.
                            buf.setLength(importformat ? 0 : 2);
                            formatKeyValue(buf, tsdb, importformat, kv, base_time, metric);
                            if (buf.length() > 0) {
                                buf.append('\n');

                                exportedData = buf.toString();
                                System.out.print(exportedData);

                                //System.out.print(buf);
                            }
                        }

                        if (exportedData != null) {
                            importLines(tsdb, client, exportedData, newTagValuePair);

                            if (delete) {
                                final DeleteRequest del = new DeleteRequest(table, key);
                                client.delete(del);
                            }
                        }
                    }
                }
            }
        }
    }

    static volatile boolean throttle = false;

    private static void importLines(final TSDB tsdb, final HBaseClient client, final String exportedData, final String newTagValuePair) {
        // scaner to go through lines
        java.util.Scanner scanner = new java.util.Scanner(exportedData);
        String line = null;

        try {
            final class Errback implements Callback<Object, Exception> {

                public Object call(final Exception arg) {
                    if (arg instanceof PleaseThrottleException) {
                        final PleaseThrottleException e = (PleaseThrottleException) arg;
                        LOG.warn("Need to throttle, HBase isn't keeping up.", e);
                        throttle = true;
                        final HBaseRpc rpc = e.getFailedRpc();
                        if (rpc instanceof PutRequest) {
                            client.put((PutRequest) rpc);  // Don't lose edits.
                        }
                        return null;
                    }
                    LOG.error("Exception caught while processing line "
                            + exportedData + " " + newTagValuePair, arg);
                    System.exit(2);
                    return arg;
                }

                public String toString() {
                    return "importLine errback";
                }
            };
            final Errback errback = new Errback();
            while (scanner.hasNextLine()) {
                line = scanner.nextLine();

                // check whether we are not looping over already modified data
                if (line.contains(newTagValuePair)) {
                    throw new RuntimeException("line " + line + " already contain new tag value pair");
                }

                String wholeImportLine = line + " " + newTagValuePair;
                LOG.info("importing line:" + wholeImportLine);

                final String[] words = Tags.splitString(wholeImportLine, ' ');
                final String metric = words[0];
                if (metric.length() <= 0) {
                    throw new RuntimeException("invalid metric: " + metric);
                }
                final long timestamp;
                try {
                    timestamp = Tags.parseLong(words[1]);
                    if (timestamp <= 0) {
                        throw new RuntimeException("invalid timestamp: " + timestamp);
                    }
                } catch (final RuntimeException e) {
                    throw e;
                }

                final String value = words[2];
                if (value.length() <= 0) {
                    throw new RuntimeException("invalid value: " + value);
                }

                try {
                    final HashMap<String, String> tags = new HashMap<String, String>();
                    for (int i = 3; i < words.length; i++) {
                        if (!words[i].isEmpty()) {
                            Tags.parse(tags, words[i]);
                        }
                    }

                    final WritableDataPoints dp = getDataPoints(tsdb, metric, tags);
                    Deferred<Object> d;
                    if (Tags.looksLikeInteger(value)) {
                        d = dp.addPoint(timestamp, Tags.parseLong(value));
                    } else {  // floating point value
                        d = dp.addPoint(timestamp, Float.parseFloat(value));
                    }
                    d.addErrback(errback);
                    if (throttle) {
                        LOG.info("Throttling...");
                        long throttle_time = System.nanoTime();
                        try {
                            d.joinUninterruptibly();
                        } catch (final Exception e) {
                            throw new RuntimeException("Should never happen", e);
                        }
                        throttle_time = System.nanoTime() - throttle_time;
                        if (throttle_time < 1000000000L) {
                            LOG.info("Got throttled for only " + throttle_time
                                    + "ns, sleeping a bit now");
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                throw new RuntimeException("interrupted", e);
                            }
                        }
                        LOG.info("Done throttling...");
                        throttle = false;
                    }
                } catch (final RuntimeException e) {
                    throw e;
                }
            }
        } catch (RuntimeException e) {
            LOG.error("Exception caught while processing line "
                    + exportedData, e);
            throw e;
        }
    }

    private static final HashMap<String, WritableDataPoints> datapoints
            = new HashMap<String, WritableDataPoints>();

    private static WritableDataPoints getDataPoints(final TSDB tsdb,
            final String metric,
            final HashMap<String, String> tags) {
        final String key = metric + tags;
        WritableDataPoints dp = datapoints.get(key);
        if (dp != null) {
            return dp;
        }
        dp = tsdb.newDataPoints();
        dp.setSeries(metric, tags);
        dp.setBatchImport(true);
        datapoints.put(key, dp);
        return dp;
    }

    private static void formatKeyValue(final StringBuilder buf,
            final TSDB tsdb,
            final boolean importformat,
            final KeyValue kv,
            final long base_time,
            final String metric) {

        final String tags;
        if (importformat) {
            final StringBuilder tagsbuf = new StringBuilder();
            for (final Map.Entry<String, String> tag
                    : Internal.getTags(tsdb, kv.key()).entrySet()) {
                tagsbuf.append(' ').append(tag.getKey())
                        .append('=').append(tag.getValue());
            }
            tags = tagsbuf.toString();
        } else {
            tags = null;
        }

        final byte[] qualifier = kv.qualifier();
        final byte[] value = kv.value();
        final int q_len = qualifier.length;

        if (q_len % 2 != 0) {
            if (!importformat) {
                // custom data object, not a data point
                if (kv.qualifier()[0] == Annotation.PREFIX()) {
                    appendAnnotation(buf, kv, base_time);
                } else {
                    buf.append(Arrays.toString(value))
                            .append("\t[Not a data point]");
                }
            }
        } else if (q_len == 2 || q_len == 4 && Internal.inMilliseconds(qualifier)) {
            // regular data point
            final Internal.Cell cell = Internal.parseSingleValue(kv);
            if (cell == null) {
                throw new IllegalDataException("Unable to parse row: " + kv);
            }
            if (!importformat) {
                appendRawCell(buf, cell, base_time);
            } else {
                buf.append(metric).append(' ');
                appendImportCell(buf, cell, base_time, tags);
            }
        } else {
            // compacted column
            final ArrayList<Internal.Cell> cells = Internal.extractDataPoints(kv);
            if (!importformat) {
                buf.append(Arrays.toString(kv.qualifier()))
                        .append('\t')
                        .append(Arrays.toString(kv.value()))
                        .append(" = ")
                        .append(cells.size())
                        .append(" values:");
            }

            int i = 0;
            for (Internal.Cell cell : cells) {
                if (!importformat) {
                    buf.append("\n    ");
                    appendRawCell(buf, cell, base_time);
                } else {
                    buf.append(metric).append(' ');
                    appendImportCell(buf, cell, base_time, tags);
                    if (i < cells.size() - 1) {
                        buf.append("\n");
                    }
                }
                i++;
            }
        }
    }
}
