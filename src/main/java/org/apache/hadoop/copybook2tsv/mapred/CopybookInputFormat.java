package org.apache.hadoop.copybook2tsv.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CopybookInputFormat extends FileInputFormat<NullWritable, Text> {
	  private static final Log LOG = LogFactory.getLog(CopybookInputFormat.class.getName());

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
	@Override
	public CopybookFileRecordReader createRecordReader( InputSplit split, TaskAttemptContext context) 
			throws IOException, InterruptedException {
		CopybookFileRecordReader reader = new CopybookFileRecordReader();
		reader.initialize(split, context);
	return reader;
	}
	

}
