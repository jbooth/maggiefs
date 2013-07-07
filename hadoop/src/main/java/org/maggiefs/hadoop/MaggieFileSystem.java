package org.maggiefs.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Wrapper for maggiefs for use by Hadoop.
 * 
 * fs.default.name should be of the form mfs://nnhost:nnport/localMountPoint.  This allows us to avoid any further configuration,
 * by either delegating to our namenode or our local mount for all FS operations.
 *
 */
public class MaggieFileSystem extends FileSystem {
	// used by hadoop  
	private Path workingDir;
	private URI name;
	private FileSystem raw;
	// used by us
	private Path mountPoint;
	private String nameHost;
	private int namePort;
	  
	  public MaggieFileSystem() {
	    workingDir = new Path(System.getProperty("user.dir")).makeQualified(this);
	  }
	  
	  private Path makeAbsolute(Path f) {
	    if (f.isAbsolute()) {
	      return f;
	    } else {
	      return new Path(workingDir, f);
	    }
	  }
	  
	  /** Adds the mountpoint in front of the path */
	  private Path lookup(Path path) {
	    checkPath(path);
	    if (!path.isAbsolute()) {
	      path = new Path(getWorkingDirectory(), path);
	    }
	    return new Path(this.mountPoint,path);
	  }

	  public URI getUri() { return name; }
	  
	  public void initialize(URI uri, Configuration conf) throws IOException {
	    super.initialize(uri, conf);
	    setConf(conf);
	    this.raw = FileSystem.getLocal(conf).getRaw();
	    this.name = uri;
	    if (! uri.getPath().startsWith("/")) {
	    	throw new RuntimeException("Mountpoint must be absolute!");
	    }
	    this.mountPoint = new Path(uri.getPath());
	    this.nameHost = uri.getHost();
	    this.namePort = uri.getPort();
	  }
	  
	  @Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
			long len) throws IOException {
		// TODO Auto-generated method stub
		return super.getFileBlockLocations(file, start, len);
	}
	
	  // delegate methods
	@Override
	public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
			throws IOException {
		return raw.append(lookup(arg0),arg1,arg2);
	}

	@Override
	public FSDataOutputStream create(Path arg0, FsPermission arg1,
			boolean arg2, int arg3, short arg4, long arg5, Progressable arg6)
			throws IOException {
		return raw.create(lookup(arg0),arg1,arg2,arg3,arg4,arg5,arg6);
	}

	@Override
	public boolean delete(Path arg0) throws IOException {
		return raw.delete(lookup(arg0));
	}

	@Override
	public boolean delete(Path arg0, boolean arg1) throws IOException {
		return raw.delete(lookup(arg0),arg1);
	}

	@Override
	public FileStatus getFileStatus(Path arg0) throws IOException {
		return raw.getFileStatus(lookup(arg0));
	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}

	@Override
	public FileStatus[] listStatus(Path arg0) throws IOException {
		return raw.listStatus(lookup(arg0));
	}

	@Override
	public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
		return raw.mkdirs(lookup(arg0),arg1);
	}

	@Override
	public FSDataInputStream open(Path arg0, int arg1) throws IOException {
		return raw.open(lookup(arg0),arg1);
	}

	@Override
	public boolean rename(Path arg0, Path arg1) throws IOException {
		return raw.rename(lookup(arg0), lookup(arg1));
	}

	@Override
	public void setWorkingDirectory(Path arg0) {
		this.workingDir = arg0;
	}

}
