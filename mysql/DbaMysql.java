/**
 * @author DELL_pc
 *  @date 2017年6月27日
 * 
 */
package com.beifeng.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;

/**
 * @author DELL_pc
 *  @date 2017年6月27日
 */
public class DbaMysql {
     public static class DBAccessMapper extends MapReduceBase	 implements Mapper<LongWritable,StudentRecord , IntWritable, Text>
     {

		
		@Override
		public void map(LongWritable key, StudentRecord value, OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			output.collect(new IntWritable(value.id), new Text(value.toString()));
		   
		}

    	 
     }
      public static class DBAccessReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
      {

		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			  while (values.hasNext()) {
			      output.collect(key, values.next());
			    }
		}
    	  
      }
     public static void main(String[] args) {
		Configuration configuration=new Configuration();
		JobConf jobConf=new JobConf(configuration);
		
		jobConf.setOutputKeyClass(IntWritable.class);
		jobConf.setOutputValueClass(Text.class);
		jobConf.setInputFormat(DBInputFormat.class);
		String[] fields={"id","name"};
		DBInputFormat.setInput(jobConf, StudentRecord.class, "user", null, null,fields );
		DBConfiguration.configureDB(jobConf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test","root","123456");
		jobConf.setMapperClass(DBAccessMapper.class);
		jobConf.setReducerClass(DBAccessReduce.class);
	    FileOutputFormat.setOutputPath(jobConf,new Path("/data/out"));
	    try {
			JobClient.runJob(jobConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
