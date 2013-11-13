package edu.arizona.cs.gt.search;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Shell;

public class GTToolInvokerMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {

		String[] indexLineStrings = value.toString().split("\t");
		String dbName = indexLineStrings[0];
		String indexName = indexLineStrings[1];

		Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
		FileSystem dfsFileSystem = FileSystem.get(context.getConfiguration());

		/*
		 * Create a temp directory to store the input files for GT tool
		 */
		File workDir = new File(System.getProperty("java.io.tmpdir") + File.separator + dbName);
		if (!workDir.exists()) {
			workDir.mkdirs();
		}

		String gtSearchQueryLocation = context.getConfiguration().get("gt.search.query");
		String gtToolLocation = context.getConfiguration().get("gt.loc");
		String gtStrand = context.getConfiguration().get("gt.search.strand", "fp");

		Path parentPath = new Path(inputPath.getParent(), dbName);
		FileUtil.copy(dfsFileSystem, new Path(parentPath, indexName + ".mer"), new File(workDir, indexName + ".mer"),
				false, context.getConfiguration());
		FileUtil.copy(dfsFileSystem, new Path(parentPath, indexName + ".mbd"), new File(workDir, indexName + ".mbd"),
				false, context.getConfiguration());
		FileUtil.copy(dfsFileSystem, new Path(parentPath, indexName + ".mct"), new File(workDir, indexName + ".mct"),
				false, context.getConfiguration());

		Path queryFilesPath = new Path(gtSearchQueryLocation);

		FileStatus[] fileStatuses = dfsFileSystem.globStatus(queryFilesPath);

		for (FileStatus queryFileStatus : fileStatuses) {

			String queryFastName = queryFileStatus.getPath().getName().split("\\.")[0];
			/*
			 * Skip comparing to the index of same file
			 */
			if (dbName.equals(queryFastName)) {
				continue;
			}

			/*
			 * Copy query file to local file system and run GT tool
			 */
			File queryFile = new File(workDir, queryFileStatus.getPath().getName());
			FileUtil.copy(dfsFileSystem, queryFileStatus.getPath(), queryFile, false, context.getConfiguration());

			File outputFile = new File(workDir, queryFastName + "-" + dbName + ".repeats");
			Shell.execCommand("sh", "-c", String.format(
					"\"%s\" \"tallymer\" \"search\" \"-output\" \"qseqnum\" \"qpos\" \"counts\""
							+ " \"-strand\" \"%s\" \"-tyr\" \"%s\" \"-q\" \"%s\" > %s", gtToolLocation, gtStrand,
					workDir.getAbsolutePath() + File.separator + indexName, queryFile.getAbsolutePath(),
					outputFile.getAbsolutePath()));

			/*
			 * Copy results back to HDFS
			 */
			Path outputPath = FileOutputFormat.getOutputPath(context);

			Path searchFileOutputPath = new Path(outputPath, outputFile.getName());
			if (dfsFileSystem.exists(searchFileOutputPath)) {
				dfsFileSystem.delete(searchFileOutputPath, false);
			}

			FileUtil.copy(outputFile, dfsFileSystem, outputPath, false, context.getConfiguration());

			/*
			 * Compute Mode file
			 */
			BufferedReader bufferedReader;
			String lineString;

			/*
			 * Read all fasta headers from the query file
			 */
			GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(queryFile));
			bufferedReader = new BufferedReader(new InputStreamReader(gzip));

			ArrayList<String> sequenceHeadersArrayList = new ArrayList<String>();
			while ((lineString = bufferedReader.readLine()) != null) {
				if (lineString.startsWith(">")) {
					sequenceHeadersArrayList.add(lineString.substring(1));
				}
			}
			bufferedReader.close();

			/*
			 * Read the search output and form a list of count for each
			 * sequenceindex
			 */
			HashMap<Integer, ArrayList<Integer>> sequenceIndexCountListMap = new HashMap<Integer, ArrayList<Integer>>();
			bufferedReader = new BufferedReader(new FileReader(outputFile));
			while ((lineString = bufferedReader.readLine()) != null) {
				String[] tabSeparatedValueStrings = lineString.split("\t");
				Integer sequenceIndexInteger = Integer.valueOf(tabSeparatedValueStrings[0]);
				Integer countInteger = Integer.valueOf(tabSeparatedValueStrings[2]);
				if (!sequenceIndexCountListMap.containsKey(sequenceIndexInteger)) {
					sequenceIndexCountListMap.put(sequenceIndexInteger, new ArrayList<Integer>());
				}
				sequenceIndexCountListMap.get(sequenceIndexInteger).add(countInteger);
			}
			bufferedReader.close();

			/*
			 * Write the mode file
			 */
			File modeFile = new File(workDir, queryFastName + "-" + dbName + ".mode");
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(modeFile));

			int mode;
			for (int i = 0; i < sequenceHeadersArrayList.size(); i++) {
				if (sequenceIndexCountListMap.get(i) != null && (mode = getMode(sequenceIndexCountListMap.get(i))) > 1
						&& mode < 1000) {
					bufferedWriter.write(String.format("%s\t%d\t%d\n", sequenceHeadersArrayList.get(i), i, mode));
				}
			}
			bufferedWriter.close();

			Path modeFileOutputPath = new Path(outputPath, modeFile.getName());
			if (dfsFileSystem.exists(modeFileOutputPath)) {
				dfsFileSystem.delete(modeFileOutputPath, false);
			}

			FileUtil.copy(modeFile, dfsFileSystem, outputPath, false, context.getConfiguration());

			queryFile.delete();
			outputFile.delete();
			modeFile.delete();
		}

		FileUtils.deleteDirectory(workDir);

		// context.write(key, new Text(fileStatuses.length + ""));

	}

	/*
	 * Finds the mode of a list of integers
	 */
	private Integer getMode(ArrayList<Integer> list) {

		TreeMap<Integer, Integer> treeMap = new TreeMap<Integer, Integer>();
		for (int listElement : list) {
			Integer frequency = treeMap.get(listElement);
			treeMap.put(listElement, frequency == null ? 1 : frequency + 1);
		}

		int max = 0;
		int mode = 0;

		for (Map.Entry<Integer, Integer> entry : treeMap.entrySet()) {
			if (entry.getValue() >= max) {
				mode = entry.getKey();
				max = entry.getValue();
			}
		}

		return mode;
	}
}
