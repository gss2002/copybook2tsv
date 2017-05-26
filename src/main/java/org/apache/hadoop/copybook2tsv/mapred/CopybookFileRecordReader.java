/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	String includeRecordTypeValue;
	String includeRecordTypeName;
	String excludeRecordTypeValue;
	String excludeRecordTypeName;
	String[] includeRecordTypeNameArray;
	String[] excludeRecordTypeNameArray;
	boolean includeIsArray = false;
	boolean excludeIsArray = false;
	boolean useIncludeRecord;
	boolean useExcludeRecord;
	boolean useRecord;
	boolean useRecLength;
	String recordLength;

	boolean mrDebug;
	boolean mrTrace;
	boolean mrTraceAll;

	String copybookName;
	int copyBookFileType;
	int splitOption;
	int copybookSysType;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		this.file = fileSplit.getPath();

		this.copybookSysType = conf.getInt("copybook2tsv.copybookNumericType", Convert.FMT_MAINFRAME);
		this.splitOption = conf.getInt("copybook2tsv.splitOption", CopybookLoader.SPLIT_NONE);
		this.copyBookFileType = conf.getInt("copybook2tsv.copybookFileType", Constants.IO_VB);
		this.copybookName = conf.get("copybook2tsv.copybook");
		this.includeRecordTypeValue = conf.get("copybook2tsv.include.recTypeValue");
		this.includeRecordTypeName = conf.get("copybook2tsv.include.recTypeName");
		this.excludeRecordTypeValue = conf.get("copybook2tsv.exclude.recTypeValue");
		this.excludeRecordTypeName = conf.get("copybook2tsv.exclude.recTypeName");
		this.recordLength = conf.get("copybook2tsv.recordLength");
		this.useIncludeRecord = conf.getBoolean("copybook2tsv.include.useRecord", true);
		this.useExcludeRecord = conf.getBoolean("copybook2tsv.exclude.useRecord", false);
		this.useRecLength = conf.getBoolean("copybook2tsv.useRecordLength", true);

		this.mrDebug = conf.getBoolean("copybook2tsv.debug", false);
		this.mrTrace = conf.getBoolean("copybook2tsv.trace", false);
		this.mrTraceAll = conf.getBoolean("copybook2tsv.traceall", false);

		LOG.info("Copybook SysType:" + copybookSysType);
		LOG.info("Copybook Split:" + splitOption);
		LOG.info("Copybook FileType:" + copyBookFileType);
		LOG.info("Copybook RecordLayout File:" + copybookName);
		LOG.info("Copybook UseRecordLength: " + useRecLength);
		if (useRecLength) {
			LOG.info("Copybook Record Length: " + recordLength);
		}
		LOG.info("Copybook UseIncludeRecord: " + useIncludeRecord);
		if (useIncludeRecord) {
			LOG.info(
					"Copybook includeRecordType Name::Value: " + includeRecordTypeName + "::" + includeRecordTypeValue);
		}

		LOG.info("Copybook UseExcludeRecord: " + useExcludeRecord);
		if (useExcludeRecord) {
			LOG.info(
					"Copybook excludeRecordType Name::Value: " + excludeRecordTypeName + "::" + excludeRecordTypeValue);
		}

		String[] loggers = { "org.apache.hadoop.copybook2tsv.mapred.CopybookFileRecordReader" };

		if (mrDebug) {
			for (String ln : loggers) {
				LOG.info("Enabling Debug");
				org.apache.log4j.Logger.getLogger(ln).setLevel(org.apache.log4j.Level.DEBUG);
			}
		}

		if (mrTrace || mrTraceAll) {
			for (String ln : loggers) {
				LOG.info("Enabling Trace");
				org.apache.log4j.Logger.getLogger(ln).setLevel(org.apache.log4j.Level.TRACE);
			}
		}

		this.ioProvider = CobolIoProvider.getInstance();

		try {
			this.reader = this.ioProvider.getLineReader(this.copyBookFileType, this.copybookSysType, this.splitOption,
					this.copybookName, this.file, this.conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (useIncludeRecord || useExcludeRecord) {
			if (useIncludeRecord) {
				if (includeRecordTypeName.contains(",")) {
					includeIsArray = true;
					includeRecordTypeNameArray = includeRecordTypeName.split(",");
					LOG.info("includeRecordTypeName::RecordNameArray Length: " + includeRecordTypeName + "::"
							+ includeRecordTypeNameArray.length);
				}
			}
			if (useExcludeRecord) {
				if (excludeRecordTypeName.contains(",")) {
					excludeIsArray = true;
					excludeRecordTypeNameArray = excludeRecordTypeName.split(",");
					LOG.info("excludeRecordTypeName::RecordNameArray Length " + excludeRecordTypeName + "::"
							+ excludeRecordTypeNameArray.length);
				}
			}
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		AbstractLine copyRecord;
		LayoutDetail copySchema = this.reader.getLayout();
		int recordId = 0; // Assuming only one record type In the file
		String cRecord = null;
		String includeRecType = null;
		String excludeRecType = null;
		StringBuffer includeRecValue = new StringBuffer();
		StringBuffer excludeRecValue = new StringBuffer();

		StringBuffer sb = new StringBuffer();

		while ((copyRecord = this.reader.read()) != null) {
			lineNum += 1;
			excludeRecValue.setLength(0);
			includeRecValue.setLength(0);
			if (mrDebug) {
				LOG.debug("Record Line::ByteLength: " + lineNum + " :: " + copyRecord.getData().length);
			}
			Integer copyRecLength = copyRecord.getData().length;
			if (useRecLength) {
				LOG.info("Record Line::ByteLength: " + lineNum + " :: " + copyRecLength);
			}

			if (useIncludeRecord) {
				this.useRecord = true;
				if (includeIsArray) {
					if (includeRecordTypeNameArray.length > 1) {
						for (int i = 0; i < includeRecordTypeNameArray.length; i++) {
							if (mrTraceAll) {
								LOG.trace("includeIsArray=true, includeRecordNameArray: "
										+ includeRecordTypeNameArray[i]);
							}
							includeRecValue
									.append(copyRecord.getFieldValue(includeRecordTypeNameArray[i]).asString().trim());
						}
						includeRecType = includeRecValue.toString();
						if (mrTraceAll) {
							LOG.trace("includeIsArray=true, includeRecType_RecordValue: " + includeRecType);
						}
					}
				} else {
					if (mrTraceAll) {
						LOG.trace("includeIsArray=false, includeRecordTypeName::NonClean:" + includeRecordTypeName
								+ " includeRecordTypeName::Clean:" + includeRecordTypeName.replace(",", "").trim());
					}
					includeRecType = copyRecord.getFieldValue(includeRecordTypeName.replace(",", "").trim()).asString()
							.trim();

					if (mrTraceAll) {
						LOG.trace("includeIsArray=false, includeRecType_RecordValue: " + includeRecType);
					}
				}
			}
			if (useExcludeRecord) {
				this.useRecord = true;
				if (excludeIsArray) {
					if (excludeRecordTypeNameArray.length > 1) {
						for (int i = 0; i < excludeRecordTypeNameArray.length; i++) {
							if (mrTraceAll) {
								LOG.trace("excludeIsArray=true, excludeRecordNameArray: "
										+ excludeRecordTypeNameArray[i]);
							}
							excludeRecValue
									.append(copyRecord.getFieldValue(excludeRecordTypeNameArray[i]).asString().trim());
						}
						excludeRecType = excludeRecValue.toString();
						if (mrTraceAll) {
							LOG.trace("excludeIsArray=true, excludeRecType_RecordValue: " + excludeRecType);
						}
					}
				} else {
					if (useExcludeRecord) {
						if (mrTraceAll) {
							LOG.trace("excludeIsArray=false, excludeRecordTypeName::NonClean:" + excludeRecordTypeName
									+ " excludeRecordTypeName::Clean:" + excludeRecordTypeName.replace(",", "").trim());
						}
						excludeRecType = copyRecord.getFieldValue(excludeRecordTypeName.replace(",", "").trim())
								.asString().trim();

						if (mrTraceAll) {
							LOG.trace("excludeIsArray=false, excludeRecType_RecordValue: " + excludeRecType);
						}
					}
				}
			}

			boolean recLength = false;

			if (recordLength != null && useRecLength) {
				if (Integer.valueOf(recordLength) == copyRecLength) {
					if (mrDebug || mrTrace || mrTraceAll) {
						LOG.debug("RecordLength String::Integer" + recordLength + " :: " + copyRecLength);
					}
					recLength = true;
				}
			}

			boolean recTypeValue = false;
			if ((includeRecordTypeValue != null && useIncludeRecord)
					|| (useExcludeRecord && excludeRecordTypeValue != null)) {
				if (useIncludeRecord && useExcludeRecord) {
					if (includeRecordTypeValue.equalsIgnoreCase(includeRecType.trim())) {
						if (excludeRecordTypeValue.equalsIgnoreCase(excludeRecType.trim())) {
							if (mrDebug || mrTrace) {
								LOG.debug("ExcludeRecordTypeValue=true - ExcludeRecord:: " + excludeRecType.trim());
							}
							recTypeValue = false;
						} else {
							if (mrDebug || mrTrace || mrTraceAll) {
								LOG.debug("IncludeRecordTypeValue=true - IncludeRecord:: " + includeRecType.trim());
							}
							recTypeValue = true;
						}
					}
				}
				if (useIncludeRecord && (!(useExcludeRecord))) {
					if (includeRecordTypeValue.equalsIgnoreCase(includeRecType.trim())) {
						if (mrDebug || mrTrace || mrTraceAll) {
							LOG.debug("IncludeRecordTypeValue=true - IncludeRecord:: " + includeRecType.trim());
						}
						recTypeValue = true;
					}
				}
				if (useExcludeRecord && (!(useIncludeRecord))) {
					if (!(excludeRecordTypeValue.equalsIgnoreCase(excludeRecType.trim()))) {
						if (mrDebug || mrTrace || mrTraceAll) {
							LOG.debug("ExcludeRecordTypeValue=true - ExcludeRecord:: " + excludeRecType.trim());
						}
						recTypeValue = false;
					} else {
						recTypeValue = true;

					}
				}
			}
			if ((recTypeValue || useRecord == false || recLength == true)) {
				if (useRecord) {
					LOG.info("Line::" + lineNum + ", RecordType::" + includeRecType + " - Schema: "
							+ copySchema.getRecord(recordId).getRecordName());
				}
				int recCount = copySchema.getRecord(recordId).getFieldCount();
				sb.setLength(0);
				copySchema.getRecord(recordId);
				if (mrDebug || mrTrace || mrTraceAll) {
					LOG.debug("CopySchema FieldCount: " + recCount);
					if (LOG.isTraceEnabled()) {
						LOG.trace("CopyRecord - " + recordId + ": " + copySchema.getRecord(recordId).toString());
						if (mrTraceAll) {
							byte[] recByteArray = copyRecord.getData();
							if (copybookSysType == Convert.FMT_MAINFRAME) {
								Charset charset = Charset.forName("cp037");
								byte[] recByteArrayLE = new String(recByteArray, charset).getBytes();
								LOG.trace("Z/OS EBCIDIC RecordLine:  " + new String(recByteArray));
								LOG.trace("Z/OS EBCIDIC HexString: " + Hex.encodeHexString(recByteArray));
								LOG.trace(
										"Converted EBCIDIC to ASCII RecordLine:  " + new String(recByteArray, charset));
								LOG.trace("Converted EBCIDIC to ASCII HexString:  "
										+ Hex.encodeHexString(recByteArrayLE));
							} else {
								LOG.trace("HexArray RecordLine:  " + new String(recByteArray));
							}
						}
					}

				}
				for (int i = 0; i < copySchema.getRecord(recordId).getFieldCount(); i++) {
					FieldDetail field = copySchema.getRecord(recordId).getField(i);
					// Clean the record before passing to stringbuffer appender
					try {
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
