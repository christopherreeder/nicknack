package nicknack;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Manifest files are inputs with a list of paths to use as the real input.
 * Paths may be directories, globs, or files and will be expanded appropriately.
 * Unlike most InputFormats, this class will silently ignore missing and
 * unmatched paths in the manifest file.
 */
public class ManifestTextInputFormat extends KeyValueTextInputFormat {

	public static final Log LOG = LogFactory.getLog(ManifestTextInputFormat.class);

	/**
	 * Given a manifest Path, return an array of the paths it contains.
	 * @param manifest
	 * @param conf
	 * @return array of Paths contained in the manifest Path
	 * @throws IOException
	 */
	private Path[] manifestToPaths(Path manifest, Configuration conf) throws IOException {
		FileSystem fs = manifest.getFileSystem(conf);
		String line;
		BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(manifest)));
		List<Path> results = new ArrayList<Path>();
		while ((line = r.readLine()) != null) {
			results.add(new Path(line));
		}
		return results.toArray(new Path[results.size()]);
	}

	/**
	 * Expand a Path to an array of FileStatus objects representing:
	 * 		- the contents if path is a directory
	 * 		- matches if path is a glob
	 * 		- the file itself if path is a file
	 * @param path
	 * @param conf
	 * @return array of FileStatus objects
	 * @throws IOException
	 */
	private FileStatus[] expandPath(Path path, Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FileStatus[] matches = fs.globStatus(path);
		List<FileStatus> results = new ArrayList<FileStatus>();
		if (matches != null) {
			for (FileStatus match : matches) {
				if (match.isDirectory()) { //Was isDir() in hadoop 1
					FileStatus[] files = fs.listStatus(match.getPath());
					for (FileStatus file : files) {
						results.add(file);
					}
				} else {
					results.add(match);
				}
			}
		}
		return results.toArray(new FileStatus[results.size()]);
	}

	/**
	 * Takes the nominal job input paths as manifest files and returns all the FileStatuses
	 * @param job
	 * @return array of FileStatus objects
	 */
	protected FileStatus[] listStatus(JobConf job) throws IOException {
		FileStatus[] manifests = super.listStatus(job);
		List<FileStatus> fileStatuses = new ArrayList<FileStatus>();
		for (FileStatus manifest : manifests) {
			Path[] tmpPaths = manifestToPaths(manifest.getPath(), job);
			for (Path path : tmpPaths) {
				FileStatus[] tmpStatuses = expandPath(path, job);
				for (FileStatus status : tmpStatuses) {
					LOG.info(status);
					fileStatuses.add(status);
				}
			}
		}
		LOG.info("Total input paths from manifest : " + fileStatuses.size());
		return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
	}

}
