package org.apache.hadoop.copybook2tsv.mapred;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.Normalizer;

import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.CobolIoProvider;
import net.sf.JRecord.Numeric.Convert;

import org.apache.commons.codec.binary.Hex;
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
	String recordLength;

	boolean mrDebug;
	boolean mrTrace;
	boolean useRecord;
	boolean useRecLength;
	int copyBookFileType;
	int splitOption;
	int copybookSysType;
	String[] recordTypeNameArray;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		this.file = fileSplit.getPath();

		this.copybookSysType = conf.getInt("copybook2tsv.copybookNumericType", Convert.FMT_MAINFRAME);
		this.splitOption = conf.getInt("copybook2tsv.splitOption", CopybookLoader.SPLIT_NONE);
		this.copyBookFileType = conf.getInt("copybook2tsv.copybookFileType", Constants.IO_VB);
		this.copybookName = conf.get("copybook2tsv.copybook");
		this.recordTypeValue = conf.get("copybook2tsv.recTypeValue");
		this.recordTypeName = conf.get("copybook2tsv.recTypeName");
		this.recordLength = conf.get("copybook2tsv.recordLength");
		this.useRecord = conf.getBoolean("copybook2tsv.useRecord", true);
		this.useRecLength = conf.getBoolean("copybook2tsv.useRecordLength", true);
		this.mrDebug = conf.getBoolean("copybook2tsv.debug", false);
		this.mrTrace = conf.getBoolean("copybook2tsv.trace", false);
		// this.recordSeqIn = conf.get("copybook2tsv.recSeq");

		LOG.info("CopyBook SysType:" + copybookSysType);
		LOG.info("CopyBook Split:" + splitOption);
		LOG.info("CopyBook FileType:" + copyBookFileType);
		LOG.info("CopyBook Name:" + copybookName);
		LOG.info("Record Type Value:" + recordTypeValue);
		LOG.info("Record Type Name: " + recordTypeName);
		LOG.info("UseRecord: " + useRecord);
		LOG.info("UseRecordLength: " + useRecLength);

		String[] loggers = { "org.apache.hadoop.copybook2tsv.mapred.CopybookFileRecordReader" };

		if (mrDebug) {
			for (String ln : loggers) {
				org.apache.log4j.Logger.getLogger(ln).setLevel(org.apache.log4j.Level.DEBUG);
			}
		}

		if (mrTrace) {
			for (String ln : loggers) {
				org.apache.log4j.Logger.getLogger(ln).setLevel(org.apache.log4j.Level.TRACE);
			}
		}

		this.ioProvider = CobolIoProvider.getInstance();
		// ioProvider.getLineReader(fileStructure, numericType, splitOption,
		// args1, filename, provider)
		// AbstractLineReader reader =
		// ioProvider.getLineReader(Constants.IO_FIXED_LENGTH,
		// Convert.FMT_MAINFRAME, CopybookLoader.SPLIT_NONE, this.copybookName,
		// this.claimFile);
		try {
			this.reader = this.ioProvider.getLineReader(this.copyBookFileType, this.copybookSysType, this.splitOption,
					this.copybookName, this.file, this.conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (useRecord) {
			recordTypeNameArray = recordTypeName.split(",");
			LOG.info("RecordNameArray: " + recordTypeNameArray);
		}

		if (useRecLength) {
			LOG.info("Record Length: " + recordLength);
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
			if (mrDebug || mrTrace) {
				LOG.debug("Rec Line and Byte Length: " + lineNum + " :: " + copyRecord.getData().length);
			}
			Integer copyRecLength = copyRecord.getData().length;
			if (useRecLength) {
				LOG.info("Rec Line and Byte Length: " + lineNum + " :: " + copyRecLength);
			}

			if (useRecord) {
				if (recordTypeNameArray.length > 1) {
					for (int i = 0; i < recordTypeNameArray.length; i++) {
						if (mrDebug || mrTrace) {
							LOG.debug("Loop RecordNameArray LoopValue: " + recordTypeNameArray[i]);
						}
						recValue.append(copyRecord.getFieldValue(recordTypeNameArray[i]).asString().trim());

					}
					recType = recValue.toString();
					if (mrDebug || mrTrace) {
						LOG.debug("Loop RecType/RecValue Out: " + recType);
					}
				} else {
					if (mrDebug || mrTrace) {
						LOG.debug("ELSE LOOP recTypeName Out: Before:" + recordTypeName + " After:"
								+ recordTypeName.replace(",", "").trim());
					}
					recType = copyRecord.getFieldValue(recordTypeName.replace(",", "").trim()).asString().trim();

					if (mrDebug || mrTrace) {
						LOG.debug("ELSE LOOP recTypeValue Out: " + recType);
					}
				}
			}

			boolean recLength = false;

			if (recordLength != null && useRecLength) {
				if (Integer.valueOf(recordLength) == copyRecLength) {
					if (mrDebug || mrTrace) {
						LOG.debug("RecordLength In " + recordLength + " :: " + copyRecLength);
					}
					recLength = true;
				}
			}

			boolean recTypeValue = false;
			if (recordTypeValue != null && useRecord) {
				if (recordTypeValue.equalsIgnoreCase(recType.trim())) {
					if (mrDebug || mrTrace) {
						LOG.debug("DEBUG: RecordTypeValue Matches Record Type from File " + recType + "="
								+ recType.trim());
					}
					recTypeValue = true;
				}
			}
			if ((recTypeValue || useRecord == false || recLength == true)) {
				if (useRecord) {
					LOG.info("Line " + lineNum + " Record Type: " + recType + " - Schema: "
							+ copySchema.getRecord(recordId).getRecordName());
				}
				int recCount = copySchema.getRecord(recordId).getFieldCount();
				sb.setLength(0);
				copySchema.getRecord(recordId);
				if (mrDebug || mrTrace) {
					LOG.debug("CopySchema FieldCount: " + recCount);
					LOG.debug("CopyRecord - " + recordId + ": " + copySchema.getRecord(recordId).toString());
					byte[] recByteArray = copyRecord.getData();
					if (copybookSysType == Convert.FMT_MAINFRAME) {
						Charset charset = Charset.forName("cp037");
						byte[] recByteArrayLE = new String(recByteArray, charset).getBytes();
						LOG.trace("Z/OS EBCIDIC Record Line:  " + new String(recByteArray));
						LOG.trace("Z/OS EBCIDIC HexString: " + Hex.encodeHexString(recByteArray));
						LOG.trace("Converted EBCIDIC to ASCII Record Line:  " + new String(recByteArray, charset));
						LOG.trace("Converted EBCIDIC to ASCII HexString:  " + Hex.encodeHexString(recByteArrayLE));
					} else {
						LOG.trace("HexArray Record Line:  " + new String(recByteArray));
					}

				}
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
					// sb.append(claimRecord.getFieldValue(field).asString());

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
