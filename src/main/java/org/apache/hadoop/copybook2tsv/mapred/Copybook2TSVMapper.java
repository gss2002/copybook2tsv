package org.apache.hadoop.copybook2tsv.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.ReflectionUtils;


public class Copybook2TSVMapper extends AutoProgressMapper<NullWritable, Text, NullWritable, Text> {

	  private static final Log LOG = LogFactory.getLog(Copybook2TSVMapper.class.getName());
	public Copybook2TSVMapper() {
	  }

	  protected void setup(Context context)
	      throws IOException, InterruptedException {
	    super.setup(context);

	    Configuration conf = context.getConfiguration();

	  }
	
	
	
	@Override
	protected void map(NullWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		context.write(key, value);

		
	}
	
	
}
