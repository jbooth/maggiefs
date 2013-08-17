package org.maggiefs.hadoop;

import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility to test that we see multiple block locations for an FS. Tests that we
 * can write/read, too.
 * 
 */
public class TestFSReplicates {

	public static void main(String[] args) throws Exception {
		Path testFilePath = new Path(args[0]);
		int numReplicas = Integer.parseInt(args[1]);

		byte[] b = new byte[65536];
		FileSystem fs = FileSystem.get(new Configuration());

		// write 257 MB
		// 64kb * 16 = 1MB
		int numWrites = 16 * 257; // 257MB
		OutputStream out = fs.create(testFilePath);
		for (int i = 0; i < numWrites; i++) {
			out.write(b);
		}
		out.flush();
		out.close();
		BlockLocation[] locs = fs.getFileBlockLocations(
				fs.getFileStatus(testFilePath), 0, (200 * 1024 * 1024));
		System.out.println(locs);
		
		
	}
}
