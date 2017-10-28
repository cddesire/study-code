
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class KeyBehaviorStatTask extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(WifiKeyBehaviorStatTask.class);

    private static final String TASK_TYPE = "wifi";

    /**
     * 输入：日志解析表
     * 输出：中间宽表
     */
    private static class KeyBehaviorStatMapper extends
            Mapper<WritableComparable, DefaultHCatRecord, Text, DefaultHCatRecord> {

        /**
         * 输出的列数
         */
        private static final Integer SCHEMA_LENGTH = BASIC_STAT_TABLE_SCHEMA_LENGTH;

        /**
         * 上报日志表的schema都一样，所有的表公用一套schema
         */
        private HCatSchema inputSchema;

        private HCatSchema outputSchema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            inputSchema = HCatInputFormat.getTableSchema(context.getConfiguration());
            if (this.inputSchema == null) {
                throw new RuntimeException("The inputSchema of table " +
                        ReportLogTableEnum.INVIEW_TABLE.getTableName() + " is empty!");
            }
            outputSchema = HCatOutputFormat.getTableSchema(context.getConfiguration());
            if (this.outputSchema == null) {
                throw new RuntimeException("The outputSchema of table " +
                        ODL_ATHENA_BASIC_STAT_FMT_TABLE + " is empty!");
            }
        }

        @Override
        protected void map(WritableComparable key, DefaultHCatRecord value, Context context)
                throws IOException, InterruptedException {
            DefaultHCatRecord outputRecord = BasicStatRecordMapper.input2Output(value,
                    inputSchema, outputSchema, SCHEMA_LENGTH, null);
            if (null == outputRecord) {
                return;
            }
            String id = outputRecord.getString("id", outputSchema);
            context.write(new Text(id), outputRecord);
        }
    }

    private static class KeyBehaviorStatReducer
            extends Reducer<Text, DefaultHCatRecord, WritableComparable, DefaultHCatRecord> {
        private HCatSchema outputSchema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputSchema = HCatOutputFormat.getTableSchema(context.getConfiguration());
            if (this.outputSchema == null) {
                throw new RuntimeException("The outputSchema of table " +
                        ODL_ATHENA_BASIC_STAT_FMT_TABLE + " is empty!");
            }
        }

        @Override
        protected void reduce(Text key, Iterable<DefaultHCatRecord> values, Context context)
                throws IOException, InterruptedException {
            DefaultHCatRecord record = null;
            for (DefaultHCatRecord r: values) {
                record = r;
            }
            context.write(null, record);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        BehaviorStatJobOption jobOption = BehaviorStatJobOption.of(args);
        if (!checkOption(jobOption)) {
            LOG.error("Input arguments are invalid, args:{}", ArrayUtils.toString(args));
            return -1;
        }
        Configuration conf = getConf();
        // 设置job基本信息
        Job job = Job.getInstance(conf);
        job.setJobName("Stat wifi key behavior task");
        job.setJarByClass(WifiKeyBehaviorStatTask.class);
        job.setNumReduceTasks(5);

        // 设置输入数据
        job.setInputFormatClass(HCatInputFormat.class);
        final String dbName = jobOption.getHiveDb();
        final String filter = new StringBuilder(64)
                .append(PartitionFieldEnum.DS.getName()).append("=").append("\"")
                .append(jobOption.getDate()).append("\"").append(" and ")
                .append(PartitionFieldEnum.HH.getName()).append("=").append("\"")
                .append(jobOption.getHour()).append("\"").append(" and ")
                .append(PartitionFieldEnum.MI.getName()).append("=").append("\"")
                .append(jobOption.getMinute()).append("\"").toString();

        HCatMultipleInputs.addInput(job, dbName, ReportLogTableEnum.WIN_TABLE.getTableName(),
                filter, WifiKeyBehaviorStatMapper.class);
        HCatMultipleInputs.addInput(job, dbName, ReportLogTableEnum.INVIEW_TABLE.getTableName(),
                filter, WifiKeyBehaviorStatMapper.class);
        HCatMultipleInputs.addInput(job, dbName, ReportLogTableEnum.CLICK_TABLE.getTableName(),
                filter, WifiKeyBehaviorStatMapper.class);
        HCatMultipleInputs.addInput(job, dbName, ReportLogTableEnum.LAND_TABLE.getTableName(),
                filter, WifiKeyBehaviorStatMapper.class);
        HCatMultipleInputs.addInput(job, dbName, ReportLogTableEnum.START_DL_TABLE.getTableName(),
                filter, WifiKeyBehaviorStatMapper.class);
        HCatMultipleInputs.addInput(job, dbName, ReportLogTableEnum.FIN_DL_TABLE.getTableName(),
                filter, WifiKeyBehaviorStatMapper.class);
        HCatMultipleInputs.addInput(job, dbName, ReportLogTableEnum.FIN_INSTALL_TABLE.getTableName(),
                filter, WifiKeyBehaviorStatMapper.class);

        job.setMapperClass(WifiKeyBehaviorStatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);

        // 设置输出表
        final Map<String, String> partKVs =
                ImmutableMap.of(PartitionFieldEnum.DS.getName(), jobOption.getDate(),
                        PartitionFieldEnum.HH.getName(), jobOption.getHour(),
                        PartitionFieldEnum.MI.getName(), jobOption.getMinute(),
                        PartitionFieldEnum.TYPE.getName(), TASK_TYPE);
        HCatOutputFormat.setOutput(job,
                OutputJobInfo.create(dbName, ODL_ATHENA_BASIC_STAT_FMT_TABLE, partKVs));
        HCatOutputFormat.setSchema(job, HCatOutputFormat.getTableSchema(job.getConfiguration()));
        job.setOutputFormatClass(HCatOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private boolean checkOption(BehaviorStatJobOption jobOption) {
        if (StringUtils.isBlank(jobOption.getHiveDb()) ||
                StringUtils.isBlank(jobOption.getDate()) ||
                StringUtils.isBlank(jobOption.getHour()) ||
                StringUtils.isBlank(jobOption.getMinute())) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("KeyBehaviorStatTask args: " + ArrayUtils.toString(args));
        int res = ToolRunner.run(new Configuration(), new WifiKeyBehaviorStatTask(), args);
        System.exit(res);
    }
}
