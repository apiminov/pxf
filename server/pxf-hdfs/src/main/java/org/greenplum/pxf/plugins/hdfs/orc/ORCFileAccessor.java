package org.greenplum.pxf.plugins.hdfs.orc;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ORCFileAccessor extends BasePlugin implements Accessor {

    static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.NOOP,
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.NOT_EQUALS,
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.IN,
                    Operator.OR,
                    Operator.AND,
                    Operator.NOT
            );
    private static final TreeVisitor PRUNER = new SupportedOperatorPruner(SUPPORTED_OPERATORS);
    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    private int batchIndex;
    private long totalRowsRead;
    private long totalReadTimeInNanos;
    private Reader fileReader;
    private RecordReader rowIterator;
    private VectorizedRowBatch batch;
    private List<ColumnDescriptor> columnDescriptors;

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        columnDescriptors = context.getTupleDescription();
    }

    @Override
    public boolean openForRead() throws Exception {
        Path file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);

        fileReader = OrcFile.createReader(file, OrcFile
                .readerOptions(configuration)
                .filesystem(file.getFileSystem(configuration)));

        // Pick the schema we want to read using schema evolution
        TypeDescription readSchema = buildReadSchema(fileReader.getSchema());
        // Get the record filter in case of predicate push-down
        SearchArgument searchArgument = getSearchArgument(context.getFilterString());

        // Build the reader options
        Reader.Options options = fileReader.options()
                .schema(readSchema)
                .range(fileSplit.getStart(), fileSplit.getLength())
                .searchArgument(searchArgument, null);

        // Read the row data
        batch = readSchema.createRowBatch();
        rowIterator = fileReader.rows(options);
        return true;
    }

    /**
     * Reads the next batch for the current fragment
     *
     * @return the next batch in OneRow format, the key is the batch number, and data is the batch
     * @throws Exception when reading of the next batch occurs
     */
    @Override
    public OneRow readNextObject() throws Exception {
        final Instant start = Instant.now();
        final boolean hasNextBatch = rowIterator.nextBatch(batch);
        totalReadTimeInNanos += Duration.between(start, Instant.now()).toNanos();
        ;
        if (hasNextBatch) {
            totalRowsRead += batch.size;
            return new OneRow(new LongWritable(batchIndex++), batch);
        }
        return null; // all batches are exhausted
    }

    @Override
    public void closeForRead() throws Exception {
        if (LOG.isDebugEnabled()) {
            final long millis = TimeUnit.NANOSECONDS.toMillis(totalReadTimeInNanos);
            long average = totalReadTimeInNanos / totalRowsRead;
            LOG.debug("{}-{}: Read TOTAL of {} rows from file {} on server {} in {} ms. Average speed: {} nanoseconds",
                    context.getTransactionId(),
                    context.getSegmentId(),
                    totalRowsRead,
                    context.getDataSource(),
                    context.getServerName(),
                    millis,
                    average);
        }
        if (rowIterator != null) {
            rowIterator.close();
        }
        if (fileReader != null) {
            fileReader.close();
        }
    }

    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException();
    }

    private SearchArgument getSearchArgument(String filterString) throws Exception {
        if (StringUtils.isBlank(filterString)) {
            return null;
        }

        ORCSearchArgumentBuilder searchArgumentBuilder =
                new ORCSearchArgumentBuilder(columnDescriptors, configuration);

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        // Prune the parsed tree with valid supported operators and then
        // traverse the pruned tree with the searchArgumentBuilder to produce a
        // SearchArgument for ORC
        TRAVERSER.traverse(root, PRUNER, searchArgumentBuilder);

        // Build the SearchArgument object
        return searchArgumentBuilder.getFilterBuilder().build();
    }

    private TypeDescription buildReadSchema(TypeDescription originalSchema) {
        TypeDescription struct = TypeDescription.createStruct();

        columnDescriptors
                .stream()
                .filter(ColumnDescriptor::isProjected);


        return struct;
    }
}
