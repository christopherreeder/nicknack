package nicknack;

import org.junit.Test;
import org.apache.hadoop.mapred.JobConf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

public class ManifestTextInputFormatTest extends BaseOutputTest {
	
	private String statusToFilename(FileStatus status) {
		String[] split = status.getPath().toString().split("/");
		return split[split.length-1];
	}
	
	@Test
	public void testFindAllFiles() throws IOException {
		ManifestTextInputFormat obj = new ManifestTextInputFormat();
		Configuration conf = new Configuration();
		conf.set("mapred.input.dir", "/Users/creeder/Documents/workspace/nicknack/test-resources/manifest.txt");
		FileStatus[] testStatuses = obj.listStatus(new JobConf(conf));
		Set<String> actual = new HashSet<String>();
		for (FileStatus status : testStatuses) {
			actual.add(statusToFilename(status));
		}
		Set<String> expected = new HashSet<String>();
		expected.add("file-a.txt");
		expected.add("file-b.txt");
		assertEquals(expected, actual);
	}

}
