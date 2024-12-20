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

package com.exceeddata.sdk.vdata.spark.v3;

import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getApplyFormula;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getDbcPath;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getDensifyRowsAhead;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getInputPath;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getSamplingFrequency;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getSignalNameFormatter;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getVswBatch;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.COLUMN_DEVICE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.exceeddata.sdk.vdata.spark.v3.message.SignalNameFormatter;
import com.exceeddata.sdk.vdata.spark.v3.reader.FSUtils;

public class VDataSparkBatch implements Batch {
    private final StructType schema;
    private final Filter[] filters;
    //private final Map<String, String> properties;
    //private final CaseInsensitiveStringMap options;
    private String filePath;
    private int densifyRowsAhead;

    private String dbcPath = "";
    private boolean applyFormula =true;
    private SignalNameFormatter signalNameFormatter;

    private int samplingFrequency;

    private int vswBatch;

    private int enter;

    public VDataSparkBatch(
            final StructType schema,
            final Filter[] filters,
            final Map<String, String> properties,
            final CaseInsensitiveStringMap options,
            final int enter) {
        this.schema = schema;
        this.filters = filters;
        //this.properties = properties;
        //this.options = options;
        this.filePath = getInputPath(options);
        this.densifyRowsAhead = getDensifyRowsAhead(options);
        this.dbcPath = getDbcPath(options);
        this.applyFormula = getApplyFormula(options);
        this.signalNameFormatter = getSignalNameFormatter(options);
        this.samplingFrequency = getSamplingFrequency(options);
        this.vswBatch = getVswBatch(options);
        this.enter = enter;
    }


    @Override
    public InputPartition[] planInputPartitions() {
        System.out.println("start planInputPartitions enter:" + enter);
        System.out.println(this);
        if(enter == 1){
            return new InputPartition[] {};
        }
        try {
            List<InputPartition> list = new ArrayList<>();
            final Configuration conf = new Configuration();
/**
 * TODO
 * 过滤文件夹
 * 在获取List<String> files 之中根据filter条件把文件夹路径中符合条件的文件过滤出来
 * 按照文件名前的2个“/” 过滤出时间， xxx/zzz/yyyymmdd/hhmm/filename.vsw
 *
 *   for (final Filter filter : filters) {
 *                 if (COLUMN_TIME.equals(filter.references()[0])) {
                    DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
                    try {
                        Date date = dateFormat.parse(day + time);
                        long longTime = date.getTime();
                        System.out.println("date--" + day + time);
                        System.out.println("longtime--" + longTime);
                    }catch (Exception e){

                    }
                    if (!VDataSparkFilters.filterWithinTimeRange(filter, storageStartTime, storageEndTime)) {
                        time_not_match = true ;
                        break;
                    }
                }
            }
 */

            List<String> files = new ArrayList<String>();
            final String[] splitPath = filePath.split(",");
            for (String path : splitPath){
                List<String> subfiles = FSUtils.listFiles(conf, path, true, Integer.MAX_VALUE);
                files.addAll(subfiles);
            }

//            final List<String> files = FSUtils.listFiles(conf, filePath, true, Integer.MAX_VALUE);

/**
            并发执行listfile
            final List<String> files = FSUtils.listFiles(conf, filePath, false, Integer.MAX_VALUE);

            final Path directoryPath = new Path(filePath);
            final FileSystem fileSystem = directoryPath.getFileSystem(conf);

            final List<String> files = listFiles(fileSystem, directoryPath, true, Integer.MAX_VALUE);
            System.out.println("files size: " + files.size());

            for (int i=0; i<fileStatuses.length; ++i) {
                System.out.println("fileStatuses: " + fileStatuses[i]);
                System.out.println("getpath: " + fileStatuses[i].getPath());
                if(fileStatuses[i].isDirectory()){
                    System.out.println("isDirectory");
//                    files.addAll(FSUtils.listFiles(fileSystem, fileStatuses[i].getPath(), true, Integer.MAX_VALUE));
                }
                System.out.println("isNotDirectory");
            }
*/

            List<String> pfiles = null;
            if (files != null && files.size() > 0) {
                int fileCount = 0;
                for (int i =0 ; i < files.size(); i ++) {
                    String f  = files.get(i);

                    // filter Filename by vin start
                    Path path = new Path(f);
                    String pathname = path.getName().trim();

                    int extindex = pathname.lastIndexOf('.');
                    String filename = extindex > 0 ? pathname.substring(0, extindex) : pathname;

                    final int lineindex = filename.indexOf('_');
                    String deviceid = lineindex > 0 ? filename.substring(0, lineindex) : filename;

                    boolean device_not_match = false ;

                    for (final Filter filter : filters) {
                        if (COLUMN_DEVICE.equals(filter.references()[0]) && !pathname.endsWith(".mvsw")) {
                            if (!VDataSparkFilters.filter(filter, deviceid)) {
                                device_not_match = true;
                                break;
                            }
                        }
                    }
                    if (device_not_match ){
                        continue;//continue while loop and try next file;
                    }
                    // filter Filename by vin end

                    // TODO filter Filename by timestamp

                    if (fileCount%vswBatch ==0 ) {
                        pfiles = new ArrayList<>();
                        pfiles.add(f);
                        VDataSparkCombinedInputPartition inputPartition = new VDataSparkCombinedInputPartition(pfiles, schema, filters, densifyRowsAhead, dbcPath, applyFormula, signalNameFormatter, samplingFrequency);
                        list.add(inputPartition);
                    }else {
                        pfiles.add(f);
                    }
                    fileCount++;
                }
                System.out.println("planInputPartitions list size" + fileCount);
            }

            return list.toArray(new InputPartition[] {});
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    /*
    private List<String> listFiles (
            final FileSystem fileSystem,
            final Path path,
            final boolean recursive,
            final int maxListing) throws IOException {
        final List<String> files = new ArrayList<String>();
        final FileStatus[] fileStatuses = fileSystem.listStatus(path);

        try {

            // 1. 创建定长线程池对象 & 设置线程池线程数量固定为3
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(3);
            // 2. 创建好Runnable类线程对象 & 需执行的任务
            //        Runnable task =new Runnable(){
            //            public void run() {
            //                System.out.println("执行任务啦");
            //            }
            //        };


            for (int i = 0; i < fileStatuses.length; ++i) {
                FileStatus fileStatus = fileStatuses[i];
                if (fileStatus.isDirectory()) {
                    // hard coding
                    final String pathString = fileStatuses[i].getPath().toString();
                    // "file:/C:/exceed/公司文档/客户文档/广汽/filetest/20241118/08"
                    final String[] split = pathString.split("/");
                    if (recursive && split.length == 9) {
                        System.out.println("pathString: " + pathString);
                        Runnable task = new Runnable() {
                            public void run() {
//                                try {
//                                    files.addAll(listFiles(fileSystem, fileStatus.getPath(), true, maxListing));
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                }
                                System.out.println("runnable task");
                            }
                        };
                        // 3. 向线程池提交任务
                        fixedThreadPool.execute(task);
                    } else if (recursive) {
                        files.addAll(listFiles(fileSystem, fileStatuses[i].getPath(), true, maxListing));
                    }
                } else {
                    final String file = fileStatuses[i].getPath().toString();
                    if (!file.startsWith(".") && !file.startsWith("_") && FSUtils.acceptFile(file)) {
                        files.add(file);
                    }
                }
                if (files.size() >= maxListing) {
                    return files.subList(0, maxListing);
                }
            }
            fixedThreadPool.awaitTermination(1, TimeUnit.MINUTES);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println("files.size: " + files.size());
        return files;
    }
    */

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new VDataSparkPartitionReaderFactory();
    }
}