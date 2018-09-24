package org.greenplum.pxf.plugins.s3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.ReadAccessor;
import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor;
import org.greenplum.pxf.plugins.hdfs.ParquetResolver;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.List;

public class S3ParquetAccessor extends ParquetFileAccessor implements ReadAccessor {

    private static final Log LOG = LogFactory.getLog(S3ParquetAccessor.class);
    private PxfS3 pxfS3;
    private ParquetResolver resolver;
    private MessageType schema;

    public S3ParquetAccessor(InputData inputData) {
        super(inputData);
        pxfS3 = PxfS3.fromInputData(this.inputData);
        pxfS3.setObjectName(new String(this.inputData.getFragmentMetadata()));
        resolver = new ParquetResolver(this.inputData);
    }

    @Override
    public boolean openForRead() throws Exception {
        LOG.info("openForRead(): " + pxfS3);
        Path path = new Path(pxfS3.getS3aURI());
        Configuration conf = new Configuration();
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        schema = metadata.getFileMetaData().getSchema();
        setSchema(schema);
        setReader(new ParquetFileReader(conf, path, ParquetMetadataConverter.NO_FILTER));
        setRecordIterator();
        return iteratorHasNext();
    }

    /**
     * This overrides the parent's method, using a Resolver to set up the
     * List<OneField> that goes into the OneRow return value (which just gets passed
     * through by S3ParquetResolver). The reason for this is that the schema isn't
     * available to the Resolver, but it is here, so it makes sense to use it here.
     */
    @Override
    public OneRow readNextObject() {
        OneRow next = super.readNextObject();
        if (next == null) {
            return null;
        }

        try {
            List<OneField> oneFieldList = new ArrayList<OneField>();
            for (OneField of : resolver.getFields(next, schema)) {
                NullableOneField nof = new NullableOneField(of.type, of.val);
                oneFieldList.add(nof);
            }
            return new OneRow(null, oneFieldList);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}