package org.apache.hadoop.copybook2tsv.mapred;

import java.io.IOException;

import java.text.Normalizer;

import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.CobolIoProvider;
import net.sf.JRecord.Numeric.Convert;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CopybookFileRecordReader extends RecordReader<NullWritable, Text> {
	private static final Log LOG = LogFactory.getLog(CopybookFileRecordReader.class.getName());
	private FileSplit fileSplit;
	private Configuration conf;
	private Text value = new Text();
	private boolean processed = false;
	Path file = null;
	boolean firstRec = true;
	AbstractLineReader reader;
	CobolIoProvider ioProvider;
	int lineNum = 0;
	String recordTypeValue;
	// String recordSeqIn;
	String copybookName;
	String recordTypeName;
	boolean mrDebug;
	boolean mrTrace;
	boolean useRecord;
	int copyBookFileType;
	int splitOption;
	int copybookSysType;
	String[] recordTypeNameArray;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		this.file = fileSplit.getPath();

		this.copybookSysType = conf.getInt("mr.copybookNumericType", Convert.FMT_MAINFRAME);
		this.splitOption = conf.getInt("mr.splitOption", CopybookLoader.SPLIT_NONE);
		this.copyBookFileType = conf.getInt("mr.copybookFileType", Constants.IO_VB);
		this.copybookName = conf.get("mr.copybook");
		this.recordTypeValue = conf.get("mr.recTypeValue");
		this.recordTypeName = conf.get("mr.recTypeName");
		this.useRecord = conf.getBoolean("mr.useRecord", true);
		this.mrDebug = conf.getBoolean("mr.debug", false);
		this.mrTrace = conf.getBoolean("mr.trace", false);

		// this.recordSeqIn = conf.get("mr.recSeq");
		LOG.info("CopyBook SysType:" + copybookSysType);
		LOG.info("CopyBook Split:" + splitOption);
		LOG.info("CopyBook FileType:" + copyBookFileType);
		LOG.info("CopyBook Name:" + copybookName);
		LOG.info("Record Type Value:" + recordTypeValue);
		LOG.info("Record Type Name: " + recordTypeName);
		LOG.info("UseRecord: " + useRecord);

		this.ioProvider = CobolIoProvider.getInstance();
		try {
			this.reader = this.ioProvider.getLineReader(this.copyBookFileType, this.copybookSysType, this.splitOption,
					this.copybookName, this.file, this.conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (useRecord) {
			recordTypeNameArray = recordTypeName.split(",");
			LOG.debug("DEBUG: RecordNameArray: " + recordTypeNameArray);
			if (mrDebug) {
				LOG.info("DEBUG: RecordNameArray: " + recordTypeNameArray);
			}

		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		AbstractLine copyRecord;
		LayoutDetail copySchema = this.reader.getLayout();
		int recordId = 0; // Assuming only one record type In the file
		String cRecord = null;
		String recType = null;

		StringBuffer sb = new StringBuffer();
		StringBuffer recValue = new StringBuffer();
		while ((copyRecord = this.reader.read()) != null) {
			lineNum += 1;
			recValue.setLength(0);
			if (useRecord) {
				if (recordTypeNameArray.length > 1) {
					for (int i = 0; i < recordTypeNameArray.length; i++) {
						LOG.debug("DEBUG: Loop RecordNameArray LoopValue: " + recordTypeNameArray[i]);
						if (mrDebug) {
							LOG.info("DEBUG: Loop RecordNameArray LoopValue: " + recordTypeNameArray[i]);
						}
						recValue.append(copyRecord.getFieldValue(recordTypeNameArray[i]).asString().trim());

					}
					recType = recValue.toString();
					LOG.debug("DEBUG: Loop RecType/RecValue Out: " + recType);
					if (mrDebug) {
						LOG.info("DEBUG: Loop RecType/RecValue Out: " + recType);
					}
				} else {
					LOG.debug("DEBUG: ELSE LOOP recTypeName Out: Before:" + recordTypeName + " After:"
							+ recordTypeName.replace(",", "").trim());
					if (mrDebug) {
						LOG.info("DEBUG: ELSE LOOP recTypeName Out: Before:" + recordTypeName + " After:"
								+ recordTypeName.replace(",", "").trim());
					}
					recType = copyRecord.getFieldValue(recordTypeName.replace(",", "").trim()).asString().trim();

					LOG.debug("DEBUG: ELSE LOOP recTypeValue Out: " + recType);
					if (mrDebug) {
						LOG.info("DEBUG: ELSE LOOP recTypeValue Out: " + recType);
					}
				}
			}
			boolean recTypeValue = false;
			if (recordTypeValue != null && useRecord) {
				if (recordTypeValue.equalsIgnoreCase(recType.trim())) {
					LOG.debug("DEBUG: RecordTypeValue Matches Record Type from File " + recType + "=" + recType.trim());
					recTypeValue = true;
				}
			}
			if ((recTypeValue || useRecord == false)) {
				if (useRecord) {
					LOG.info("Line " + lineNum + " Record Type: " + recType + " - Schema: "
							+ copySchema.getRecord(recordId).getRecordName());
				}
				int recCount = copySchema.getRecord(recordId).getFieldCount();
				sb.setLength(0);
				copySchema.getRecord(recordId);
				if (mrTrace) {
					LOG.info("CopyRecord - " + recordId + ": " + copySchema.getRecord(recordId).toString());
				}
				LOG.trace("CopyRecord - " + recordId + ": " + copySchema.getRecord(recordId).toString());
				for (int i = 0; i < copySchema.getRecord(recordId).getFieldCount(); i++) {
					FieldDetail field = copySchema.getRecord(recordId).getField(i);
					// Clean the record before passing to stringbuffer appender
					try {
						// cRecord =
						// copyRecord.getFieldValue(field).asString().replaceAll("[\r\n\t]",
						// " ").trim();
						cRecord = removeBadChars(copyRecord.getFieldValue(field).asString()).trim();
						if (cRecord == null || cRecord.isEmpty()) {
							sb.append("NULL");
						} else {
							sb.append(cRecord);
						}

					} catch (StringIndexOutOfBoundsException e) {
						String fontName = copySchema.getRecord(recordId).getFontName();
						String typeName = copyRecord.getFieldValue(field).getTypeName();
						String fieldName = field.getName();
						sb.append("BAD_FIELD_RECORD");
						LOG.error("Bad Record: Line=" + lineNum + " FieldName=" + fieldName + " FieldType=" + typeName
								+ " FontName=" + fontName);

					}

					if (recCount == i) {
						// sb.append("\n");
					} else {
						sb.append("\t");
					}
				}
				value.set(sb.toString());
				return true;
			}
		}
		reader.close();
		return false;

	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

	public static String removeBadChars(String strToBeTrimmed) {
		String strout = StringUtils.replace(strToBeTrimmed, "\n\r", " ");
		strout = StringUtils.replace(strout, "\r\n", " ");
		strout = StringUtils.replace(strout, "\n", " ");
		strout = StringUtils.replace(strout, "\r", " ");
		strout = StringUtils.replace(strout, "\t", " ");
		strout = StringUtils.replace(strout, "\b", " ");
		if (!(StringUtils.isAsciiPrintable(strout))) {
			strout = Normalizer.normalize(strout, Normalizer.Form.NFD);
			strout = strout.replaceAll("[^\\x00-\\x7F]", "");
		}
		return strout;
	}

}
