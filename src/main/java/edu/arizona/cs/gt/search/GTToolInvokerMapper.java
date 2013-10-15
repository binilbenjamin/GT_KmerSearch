package edu.arizona.cs.gt.search;

import java.io.File;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Shell;

public class GTToolInvokerMapper extends Mapper<Text, Text, Text, Text> {

	protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		FileSplit split = (FileSplit) context.getInputSplit();
		Path inputPath = split.getPath();

		/*
		 * Create a temp directory to store the input files for GT tool
		 */
		File tempDir = new File(System.getProperty("java.io.tmpdir") + File.separator + key);
		if (!tempDir.exists()) {
			tempDir.mkdirs();
		}

		String gtSearchQueryLocation = context.getConfiguration().get("gt.search.query");
		String gtToolLocation = context.getConfiguration().get("gt.loc");
		String gtStrand = context.getConfiguration().get("gt.search.strand", "fp");

		Path parentPath = new Path(inputPath.getParent(), key.toString());
		FileUtil.copy(FileSystem.get(context.getConfiguration()), new Path(parentPath, value + ".mer"), new File(
				tempDir, value + ".mer"), false, context.getConfiguration());
		FileUtil.copy(FileSystem.get(context.getConfiguration()), new Path(parentPath, value + ".mbd"), new File(
				tempDir, value + ".mbd"), false, context.getConfiguration());
		FileUtil.copy(FileSystem.get(context.getConfiguration()), new Path(parentPath, value + ".mct"), new File(
				tempDir, value + ".mct"), false, context.getConfiguration());

		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		Path queryFilesPath = new Path(gtSearchQueryLocation);

		FileStatus[] fileStatuses = fileSystem.globStatus(queryFilesPath);

		for (FileStatus queryFileStatus : fileStatuses) {

			/*
			 * Skip comparing to the index of same file
			 */
			if (key.toString().equals(queryFileStatus.getPath().getName())) {
				continue;
			}

			/*
			 * Copy query file to local file system and run GT tool
			 */
			File queryFile = new File(tempDir, queryFileStatus.getPath().getName());
			FileUtil.copy(fileSystem, queryFileStatus.getPath(), queryFile, false, context.getConfiguration());

			File outputFile = new File(tempDir, queryFileStatus.getPath().getName() + "-" + key);
			Shell.execCommand("sh", "-c", String.format(
					"\"%s\" \"tallymer\" \"search\" \"-output\" \"qseqnum\" \"qpos\" \"counts\""
							+ " \"-strand\" \"%s\" \"-tyr\" \"%s\" \"-q\" \"%s\" > %s", gtToolLocation, gtStrand,
					tempDir.getAbsolutePath() + File.separator + value, queryFile.getAbsolutePath(),
					outputFile.getAbsolutePath()));

			/*
			 * Copy results back to HDFS
			 */
			Path destinationPath = FileOutputFormat.getOutputPath(context);
			FileUtil.copy(outputFile, fileSystem, destinationPath, false, context.getConfiguration());

			queryFile.delete();
			outputFile.delete();

		}

		// context.write(key, new Text(fileStatuses.length + ""));

	}
}
