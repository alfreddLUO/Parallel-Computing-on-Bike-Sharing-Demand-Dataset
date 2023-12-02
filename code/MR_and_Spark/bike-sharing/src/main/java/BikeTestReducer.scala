import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer


/**
 * @author Harry
 * @since 2023/11/17 22:33
 */
class BikeTestReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override
  def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.iterator().next().get()
    context.write(key, new IntWritable(sum))
  }
}
