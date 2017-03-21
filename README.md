# Copybook2TSV

### To Build Copybook2TSV:
* Build tar/gz:
	mvn package -Ptar

* Build rpm:
	mvn package -Prpm

#### Convert Copybook

	./copybook2tsv --convert2tsv --input /hdfsinputfolder/appname/2014/06 --output /hdfsoutputfolder/appname/c20/2014/06 --appname APPNAME --recordtype_name RECORD-TYPE,RECORD-TYPE-SEQ --recordtype_value C20 --copybook $HOME/copybooks/APPNAMEC20.txt --copybook_filetype MFVB --hive_partition LOAD_YEAR=2014,LOAD_MONTH=06


#### Create Hive Table Layout:

	./copybook2tsv -gen_hive_only -convert2tsv --output /hdfsoutputfolder/appname/c20/2014/06 --appname APPNAME --recordtype_name RECORD-TYPE,RECORD-TYPE-SEQ --recordtype_value C20 --copybook $HOME/copybooks/APPNAMEC20.txt --copybook_filetype MFVB --hive_partition LOAD_YEAR=2014,LOAD_MONTH=06


## Copybook2TSV Samples
##### Copy Sample Data into HDFS Only need to be done once:
	hadoop fs -copyFromLocal $HOME/copybook2tsv/samples/dtar020 ./dtar020src
	hadoop fs -copyFromLocal $HOME/copybook2tsv/samples/dtar1000 ./dtar1000src

##### Remove Output from copybook2tsv jobs for samples if run before
	hadoop fs -rmr ./dtar020output
	hadoop fs -rmr ./dtar100output

##### Run the job to convert from EBCDIC Copybook to TSV
###### FIXED BLOCK

	./copybook2tsv --convert2tsv --input ./dtar020src --output ./dtar020output --appname POC_FB --copybook $HOME/copybook2tsv/samples/DTAR020.cbl --copybook_filetype MFFB --tablename vsam

###### VARIABLE BLOCK

	./copybook2tsv --convert2tsv --input ./dtar1000src --output ./dtar1000output --appname POC_VB --copybook /home/t93kd9i/copybook2tsv/samples/DTAR1000.cbl --copybook_filetype MFVB  --tablename POC

###### Verify Output
	hadoop fs -cat ./dtar020output/* | more
	hadoop fs -cat ./dtar1000output/* | more
