/* 
 * Copyright (C) 2023-2025 Smart Software for Car Technologies Inc. and EXCEEDDATA
 *     https://www.smartsct.com
 *     https://www.exceeddata.com
 *
 *                            MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * Except as contained in this notice, the name of a copyright holder
 * shall not be used in advertising or otherwise to promote the sale, use 
 * or other dealings in this Software without prior written authorization 
 * of the copyright holder.
 */

package com.exceeddata.sdk.vdata.spark.v2.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class FSUtils {
    private FSUtils() {}
    
    /**
     * List the files.
     * 
     * @param conf the configuration
     * @param directory the configuration
     * @param recursive whether to recursively search
     * @param maxListing max to list
     * @return List
     * @throws IOException throws IOException if problems occur
     */
    public static final List<String> listFiles (
            final Configuration conf, 
            final String directory, 
            final Boolean recursive,
            final int maxListing) throws IOException {
        if (directory == null || directory.trim().length() == 0) {
            return new ArrayList<String>();
        }
        
        final Path directoryPath = new Path(directory);
        final FileSystem fileSystem = directoryPath.getFileSystem(conf);
        
        if (!fileSystem.exists(directoryPath)) {
            return new ArrayList<String>();
        }
        
        return listFiles(fileSystem, directoryPath, recursive, maxListing);
    }
    
    private static final List<String> listFiles (
            final FileSystem fileSystem, 
            final Path path, 
            final boolean recursive,
            final int maxListing) throws IOException {
        final List<String> files = new ArrayList<String>();
        final FileStatus[] fileStatuses = fileSystem.listStatus(path);
        
        for (int i=0; i<fileStatuses.length; ++i) {
            if (fileStatuses[i].isDirectory()) {
                if (recursive) {
                    files.addAll(listFiles (fileSystem, fileStatuses[i].getPath(), true, maxListing));
                }
            }  else {
                final String file = fileStatuses[i].getPath().toString();
                if (!file.startsWith(".") && !file.startsWith("_") && acceptFile(file)) {
                    files.add(file);
                }
            }
            if (files.size() >= maxListing) {
                return files.subList(0, maxListing);
            }
        }
        
        return files;
    }
        
    public static boolean acceptFile(final String f) {
        final String name = f.toLowerCase();
        return name.endsWith(".vsw") || name.endsWith(".stf") || name.endsWith(".mmf")|| name.endsWith(".sdt");
    }
}
