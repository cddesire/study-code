import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;


public class HCatDelegatingMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {
    private Mapper<K1, V1, K2, V2> mapper;

    @Override
    @SuppressWarnings("unchecked")
    protected void setup(Context context)
            throws IOException, InterruptedException {
        HCatMultipleInputs.InputInfo input = HCatMultipleInputs.readInputInfoFromSplit(context.getInputSplit());
        Class<? extends Mapper> mapperClass;
        try {
            if (input.mapperClass != null) {
                mapperClass = (Class<? extends Mapper>) Class.forName(input.mapperClass);
            } else {
                throw new NullPointerException("Mapper not set for input: " + input.table);
            }
            mapper = (Mapper<K1, V1, K2, V2>) ReflectionUtils.newInstance(mapperClass, context.getConfiguration());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        mapper.run(context);
        cleanup(context);
    }
}
