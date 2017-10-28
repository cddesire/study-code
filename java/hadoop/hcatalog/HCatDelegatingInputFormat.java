import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

class HCatDelegatingInputFormat extends HCatBaseInputFormat {

    private void setInput(Configuration conf, HCatMultipleInputs.InputInfo info) throws IOException {
        String table = info.table;
        String dbName = info.dbName;
        String filter = info.filter;
        HCatInputFormat.setInput(conf, dbName, table).setFilter(filter);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        List<InputSplit> result = new ArrayList<>();
        Configuration ctxConf = jobContext.getConfiguration();
        for (HCatMultipleInputs.InputInfo input : HCatMultipleInputs.getTableInfoSet(ctxConf)) {
            Configuration conf = new Configuration(ctxConf);
            setInput(conf, input);
            JobContext ctx = Job.getInstance(conf);
            List<InputSplit> splits = super.getSplits(ctx);
            for (InputSplit split : splits) {
                HCatMultipleInputs.writeInputInfoToSplit(split, input);
            }
            result.addAll(splits);

        }
        return result;
    }

    @Override
    public RecordReader<WritableComparable, HCatRecord> createRecordReader(
            InputSplit split, TaskAttemptContext taskContext) throws IOException, InterruptedException {
        HCatMultipleInputs.InputInfo input = HCatMultipleInputs.readInputInfoFromSplit(split);
        setInput(taskContext.getConfiguration(), input);
        return super.createRecordReader(split, taskContext);
    }

}