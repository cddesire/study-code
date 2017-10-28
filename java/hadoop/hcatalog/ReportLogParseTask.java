
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.MultiOutputFormat;
import org.apache.hive.hcatalog.mapreduce.MultiOutputFormat.JobConfigurer;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;


/**
 * 日志解析
 */
public class ReportLogParseTask extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(ReportLogParseTask.class);

    private static final String IDEA_MAPPING_FILE = "mapping_file";

    // 表的列数
    private static final Integer SCHEMA_LENGTH = 20;

    private static final String TASK_RUNNING_DATE = "task_running_date";

    /**
     * 解析类日志
     */
    private static class ReportLogParseMapper extends
            Mapper<Object, Text, Text, DefaultHCatRecord> {

        private static final Integer INITIAL_SIZE = 15000;
        
        private static Map<String, String> ideaTargetMap =
                Maps.newHashMapWithExpectedSize(INITIAL_SIZE);

        private RandomDataGenerator r = new RandomDataGenerator();

        /**
         * 任务运行日期 yyyy-MM-dd HH:mm:00，分钟级别任务
         * 00 05 10 15 20 25 30 35 40 45 55
         */
        private String taskRunningDate;

        // 上报日志表的schema都一样，所有的表共用一套schema
        private HCatSchema schema;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            String inviewTableName = "adl_xxx_fdt";
            JobContext jobContext = MultiOutputFormat.getJobContext(inviewTableName, context);
            schema = HCatOutputFormat.getTableSchema(jobContext.getConfiguration());
            if (this.schema == null) {
                throw new RuntimeException("The schema of table " + inviewTableName + " is empty!");
            }

            taskRunningDate = context.getConfiguration().get(TASK_RUNNING_DATE);
            if (StringUtils.isBlank(taskRunningDate)) {
                throw new RuntimeException("Task running partition date is empty");
            }

            //维表
            if (null == context.getCacheFiles() || context.getCacheFiles().length < 1) {
                throw new RuntimeException("idea mapping file is empty");
            }
            List<String> lines = FileUtils.readLines(
                    new File("./" + IDEA_MAPPING_FILE));
            if (CollectionUtils.isEmpty(lines)) {
                return;
            }
            for (String line : lines) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    ideaTargetMap.put(parts[0], parts[1]);
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String log = value.toString();
            if (StringUtils.isBlank(log)) {
                return;
            }
            ReportLogBean logBean = ReportLogParse.parse(log);
            if (null == logBean) {
                return;
            }
            // 构建hive表的行
            DefaultHCatRecord record = this.buildRecord(logBean);
            // 随机的key
            String outputKey = logBean.getType() + "#" + r.nextLong(0L, Long.MAX_VALUE);
            context.write(new Text(outputKey), record);
        }

        /**
         * @param logBean
         * @return
         * @throws HCatException
         */
        private DefaultHCatRecord buildRecord(final ReportLogBean logBean) throws HCatException {
            DefaultHCatRecord record = new DefaultHCatRecord(SCHEMA_LENGTH);
            record.setString("log_time", schema, logBean.getLogTime());
            record.setString("ip", schema, logBean.getIp());
            record.setString("type", schema, logBean.getType());
            return record;
        }

    }

    /**
     * 按照表分到不同的reducer上
     */
    private static class TableHashPartitioner extends Partitioner<Text, DefaultHCatRecord> {
        private RandomDataGenerator r = new RandomDataGenerator();
        @Override
        public int getPartition(Text key, DefaultHCatRecord value, int numReduceTasks) {
            // 取日志类型，对应的表
            String[] splits = key.toString().split("#");
            String type = splits[0];
            if (ReportLogTableEnum.WIN_TABLE.getType().equals(type)) {
                int rand = r.nextInt(0, 100);
                if (rand < 20) {
                    return 2;
                }
                return 0;
            } else if (ReportLogTableEnum.INVIEW_TABLE.getType().equals(type)) {
                return 1;
            } else {
                return 2;
            }
        }
    }


    /**
     * 直接输出，用于小文件合并
     */
    private static class IdentityReducer extends Reducer<Text, DefaultHCatRecord, WritableComparable, DefaultHCatRecord> {

        // 上报日志表的schema都一样，所有的表公用一套schema
        private HCatSchema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String inviewTableName = ReportLogTableEnum.INVIEW_TABLE.getTableName();
            JobContext jobContext = MultiOutputFormat.getJobContext(inviewTableName, context);
            schema = HCatOutputFormat.getTableSchema(jobContext.getConfiguration());
            if (this.schema == null) {
                throw new RuntimeException("The schema of table " + inviewTableName + " is empty!");
            }
        }

        @Override
        protected void reduce(Text key, Iterable<DefaultHCatRecord> values, Context context)
                throws IOException, InterruptedException {
            for (DefaultHCatRecord record : values) {
                String type = record.getString("type", schema);
                ReportLogTableEnum table = ReportLogTableEnum.getEmumByType(type);
                String tableName = table.getTableName();
                context.getCounter("reduce " + tableName, type).increment(1L);
                // 写入hive表
                if (!ReportLogTableEnum.UNKNOWN.equals(table)) {
                    MultiOutputFormat.write(tableName, null, record, context);
                } else {
                    LOG.error("Invalid log type, type={}", type);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        ReportParseJobOption jobOption = ReportParseJobOption.of(args);
        if (!checkOption(jobOption)) {
            System.err.printf("Please check your input param");
            return -1;
        }

        Configuration conf = getConf();
        // 设置任务运行的时间 yyyy-MM-dd HH:mm:00
        String taskRunningDate = new StringBuilder(jobOption.getDate())
                .append(" ").append(jobOption.getHour()).append(":")
                .append(jobOption.getMinute()).append(":00").toString();
        conf.set(TASK_RUNNING_DATE, taskRunningDate);
        // 设置job基本信息
        Job job = Job.getInstance(conf);
        job.setJobName("Parse report log task");
        job.setJarByClass(ReportLogParseTask.class);

        // 设置输入数据
        String[] inputPaths = jobOption.getInputPath().split(",");
        for (String path : inputPaths) {
            System.out.println("**** add input path:" + path);
            MultipleInputs.addInputPath(job, new Path(path),
                    TextInputFormat.class, ReportLogParseMapper.class);
        }
        // 添加映射文件
        job.addCacheFile(new URI(jobOption.getIdeaMappingPath() + "#" + IDEA_MAPPING_FILE));

        // 设置不同数据走向不同的reduce
        job.setPartitionerClass(TableHashPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);
        job.setNumReduceTasks(3);
        job.setReducerClass(IdentityReducer.class);
        job.setOutputFormatClass(MultiOutputFormat.class);
        JobConfigurer jc = MultiOutputFormat.createConfigurer(job);
        final Map<String, String> partKVs =
                ImmutableMap.of(PartitionFieldEnum.DS.getName(), jobOption.getDate(),
                        PartitionFieldEnum.HH.getName(), jobOption.getHour(),
                        PartitionFieldEnum.MI.getName(), jobOption.getMinute());
        final String dbName = jobOption.getHiveDb();

        ReportLogTableEnum[] tables = ReportLogTableEnum.values();
        for (ReportLogTableEnum table : tables) {
            if (!ReportLogTableEnum.UNKNOWN.equals(table)) {
                String tableName = table.getTableName();
                System.out.println("**** set table: " + tableName);
                jc.addOutputFormat(tableName, HCatOutputFormat.class, WritableComparable.class, DefaultHCatRecord.class);
                Job tableJob = jc.getJob(tableName);
                HCatOutputFormat.setOutput(tableJob, OutputJobInfo.create(dbName, tableName, partKVs));
                HCatOutputFormat.setSchema(tableJob, HCatOutputFormat.getTableSchema(tableJob.getConfiguration()));
            }
        }
        // 多分区输出配置
        jc.configure();
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 检查输入文件
     *
     * @param jobOption
     * @return
     */
    private boolean checkOption(ReportParseJobOption jobOption) {
        if (StringUtils.isBlank(jobOption.getInputPath()) ||
                StringUtils.isBlank(jobOption.getHiveDb()) ||
                StringUtils.isBlank(jobOption.getDate()) ||
                StringUtils.isBlank(jobOption.getHour()) ||
                StringUtils.isBlank(jobOption.getMinute())) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("ReportLogParseTask args: " + ArrayUtils.toString(args));
        int res = ToolRunner.run(new Configuration(), new ReportLogParseTask(), args);
        System.exit(res);
    }
}
