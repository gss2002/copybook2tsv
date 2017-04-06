package org.apache.hadoop.copybook2tsv.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Copybook2TSVMapper extends AutoProgressMapper<NullWritable, Text, NullWritable, Text> {

	public Copybook2TSVMapper() {
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

	}

	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {

		context.write(key, value);

	}

}
