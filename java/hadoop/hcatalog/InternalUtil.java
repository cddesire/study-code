import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.AbstractSerializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatSplit;
import org.apache.hive.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hive.hcatalog.mapreduce.StorerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

public class InternalUtil {

    private static final Logger LOG = LoggerFactory.getLogger(InternalUtil.class);

    static StorerInfo extractStorerInfo(StorageDescriptor sd, Map<String, String> properties)
            throws IOException {
        Properties hcatProperties = new Properties();
        for (String key : properties.keySet()) {
            hcatProperties.put(key, properties.get(key));
        }
        // also populate with StorageDescriptor->SerDe.Parameters
        for (Map.Entry<String, String> param :
                sd.getSerdeInfo().getParameters().entrySet()) {
            hcatProperties.put(param.getKey(), param.getValue());
        }
        return new StorerInfo(sd.getInputFormat(), sd.getOutputFormat(),
                sd.getSerdeInfo().getSerializationLib(),
                properties.get(META_TABLE_STORAGE), hcatProperties);
    }

    static StructObjectInspector createStructObjectInspector(HCatSchema outputSchema) throws IOException {
        if (outputSchema == null) {
            throw new IOException("Invalid output schema specified");
        }
        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        List<String> fieldNames = new ArrayList<String>();

        for (HCatFieldSchema hcatFieldSchema : outputSchema.getFields()) {
            TypeInfo type = TypeInfoUtils.getTypeInfoFromTypeString(hcatFieldSchema.getTypeString());
            fieldNames.add(hcatFieldSchema.getName());
            fieldInspectors.add(getObjectInspector(type));
        }

        StructObjectInspector structInspector = ObjectInspectorFactory.
                getStandardStructObjectInspector(fieldNames, fieldInspectors);
        return structInspector;
    }

    private static ObjectInspector getObjectInspector(TypeInfo type) throws IOException {
        switch (type.getCategory()) {
            case PRIMITIVE:
                PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
                return PrimitiveObjectInspectorFactory.
                        getPrimitiveJavaObjectInspector(primitiveType.getPrimitiveCategory());
            case MAP:
                MapTypeInfo mapType = (MapTypeInfo) type;
                MapObjectInspector mapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                        getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
                return mapInspector;
            case LIST:
                ListTypeInfo listType = (ListTypeInfo) type;
                ListObjectInspector listInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                        getObjectInspector(listType.getListElementTypeInfo()));
                return listInspector;
            case STRUCT:
                StructTypeInfo structType = (StructTypeInfo) type;
                List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();
                List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
                for (TypeInfo fieldType : fieldTypes) {
                    fieldInspectors.add(getObjectInspector(fieldType));
                }
                StructObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
                        structType.getAllStructFieldNames(), fieldInspectors);
                return structInspector;
            default:
                throw new IOException("Unknown field schema type");
        }
    }

    //TODO this has to find a better home, it's also hardcoded as default in hive would be nice
    // if the default was decided by the serde
    static void initializeOutputSerDe(AbstractSerializer serDe, Configuration conf, OutputJobInfo jobInfo)
            throws SerDeException {
        serDe.initialize(conf, getSerdeProperties(jobInfo.getTableInfo(), jobInfo.getOutputSchema()));
    }

    static void initializeDeserializer(AbstractDeserializer deserializer, Configuration conf,
                                       HCatTableInfo info, HCatSchema schema) throws SerDeException {
        Properties props = getSerdeProperties(info, schema);
        LOG.info("Initializing " + deserializer.getClass().getName() + " with properties " + props);
        deserializer.initialize(conf, props);
    }

    private static Properties getSerdeProperties(HCatTableInfo info, HCatSchema s)
            throws SerDeException {
        Properties props = new Properties();
        List<FieldSchema> fields = HCatUtil.getFieldSchemaList(s.getFields());
        props.setProperty(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS,
                MetaStoreUtils.getColumnNamesFromFieldSchema(fields));
        props.setProperty(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES,
                MetaStoreUtils.getColumnTypesFromFieldSchema(fields));

        // setting these props to match LazySimpleSerde
        props.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT, "\\N");
        props.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "1");

        //add props from params set in table schema
        props.putAll(info.getStorerInfo().getProperties());

        return props;
    }

    static Reporter createReporter(TaskAttemptContext context) {
        return new ProgressReporter(context);
    }

    /**
     * Casts an InputSplit into a HCatSplit, providing a useful error message if the cast fails.
     *
     * @param split the InputSplit
     * @return the HCatSplit
     * @throws IOException
     */
    public static HCatSplit castToHCatSplit(InputSplit split) throws IOException {
        if (split instanceof HCatSplit) {
            return (HCatSplit) split;
        } else {
            throw new IOException("Split must be " + HCatSplit.class.getName()
                    + " but found " + split.getClass().getName());
        }
    }


    static Map<String, String> createPtnKeyValueMap(Table table, Partition ptn)
            throws IOException {
        List<String> values = ptn.getValues();
        if (values.size() != table.getPartitionKeys().size()) {
            throw new IOException(
                    "Partition values in partition inconsistent with table definition, table "
                            + table.getTableName() + " has "
                            + table.getPartitionKeys().size()
                            + " partition keys, partition has " + values.size()
                            + "partition values");
        }
        Map<String, String> ptnKeyValues = new HashMap<>();
        int i = 0;
        for (FieldSchema schema : table.getPartitionKeys()) {
            // CONCERN : the way this mapping goes, the order *needs* to be
            // preserved for table.getPartitionKeys() and ptn.getValues()
            ptnKeyValues.put(schema.getName().toLowerCase(), values.get(i));
            i++;
        }

        return ptnKeyValues;
    }
}