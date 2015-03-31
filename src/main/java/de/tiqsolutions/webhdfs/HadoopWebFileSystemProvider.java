/*
 * Decompiled with CFR 0_99.
 * 
 * Could not load the following classes:
 *  org.apache.hadoop.conf.Configuration
 */
package de.tiqsolutions.webhdfs;

import de.tiqsolutions.hdfs.HadoopFileSystemProvider;
import org.apache.hadoop.conf.Configuration;

public class HadoopWebFileSystemProvider
extends HadoopFileSystemProvider {
    @Override
    public String getScheme() {
        return "webhdfs";
    }

    @Override
    protected Configuration getConfiguration() {
        Configuration conf = super.getConfiguration();
        conf.addResource("webhdfs-default.xml");
        return conf;
    }
}

