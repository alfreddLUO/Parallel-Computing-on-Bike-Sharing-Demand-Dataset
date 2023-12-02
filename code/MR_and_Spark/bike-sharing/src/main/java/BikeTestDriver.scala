import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * @author Harry
 * @since 2023/11/17 22:33
 */
object BikeTestDriver {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://master:9000")
    val job = new Job(conf, "BikeSharingMR")
    job.setJarByClass(classOf[BikeTestMapper])
    job.setMapperClass(classOf[BikeTestMapper])
    job.setCombinerClass(classOf[BikeTestReducer])
    job.setReducerClass(classOf[BikeTestReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    val outPath = new Path(args(1))
    if (outPath.getFileSystem(conf).exists(outPath)) {
      outPath.getFileSystem(conf).delete(outPath, true)
    }
    FileOutputFormat.setOutputPath(job, outPath)
    job.waitForCompletion(true)
  }

}
