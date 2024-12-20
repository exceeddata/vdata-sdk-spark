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

package com.exceeddata.sdk.vdata.spark.v3.table;

import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.COLUMN_DEVICE;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.COLUMN_TIME;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig;
import com.exceeddata.sdk.vdata.spark.v3.VDataSparkTable;

public class VDataTableCatalog implements TableCatalog {

    private String name;
    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        Identifier[] tables = new Identifier[1];
        tables [0] = new Identifier() {
            @Override
            public String[] namespace() {
                String[]  namespaces =new String[1];
                namespaces [0] ="VSW-DEMO";

                return namespaces;
            }

            @Override
            public String name() {
                return "vsw_dbc";
            }
        } ;

        return new Identifier[0];
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        Map<String, String> properties = new HashMap<>();

        String vswpath="hdfs://ld-7xv8srq5js3309my7/vsw/auto/100";
        String dbcPath = "hdfs://ld-7xv8srq5js3309my7/dbc/Dbc_c1.dbc,hdfs://ld-7xv8srq5js3309my7/dbc/Dbc_c2.dbc";

//        String vswpath = "C:\\exceed\\公司文档\\客户文档\\广汽\\vsw\\gac_copied_data\\VINSHJTEST000001_2024101217_02.vsw";
//        String dbcPath = "C:\\exceed\\公司文档\\客户文档\\广汽\\dbc\\Dbc_c1.dbc,C:\\exceed\\公司文档\\客户文档\\广汽\\dbc\\Dbc_c2.dbc";

        properties.put(VDataSparkConfig.INPUT_PATH, vswpath);
        properties.put(VDataSparkConfig.DBC_PATH, dbcPath);
        properties.put(VDataSparkConfig.SIGNAL_NAME_FORMATTER_IMPL,"com.exceeddata.sdk.vdata.spark.v3.message.SignalNameFormatterGacImpl");
        properties.put(VDataSparkConfig.SCHEMA_NUM_MAXREAD,"1");
        properties.put(VDataSparkConfig.DENSIFY_ROWS_AHEAD, "0");
//        properties.put(VDataSparkConfig.TABLE_NAME, "ztable");

        String allf = "C1m8.c1m8mid,C1m8.c1m8cid,C1m8.c1m8a1,C1m8.c1m8a2";

        String[] fs = allf.split(",");
        final StructField[] fields = new StructField[fs.length+2];
        fields[0] = new StructField(COLUMN_DEVICE, DataTypes.StringType, false, Metadata.empty());
        fields[1] = new StructField(COLUMN_TIME, DataTypes.TimestampType, false, Metadata.empty());
        StructType st = new StructType(fields);
        for (int i =0; i< fs.length; i++){
            fields[i+2] = new StructField(fs[i], DataTypes.DoubleType, true, Metadata.empty());
        }
        Table t = new VDataSparkTable(st, null, properties);

        return t;
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
        throw new TableAlreadyExistsException("Not Support");
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        return null;
    }

    @Override
    public boolean dropTable(Identifier ident) {
        return false;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {

    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }
}
