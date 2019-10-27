
package com.zhw.flume.factory;

import com.zhw.flume.source.Source;
import org.apache.flume.FlumeException;

/**
 * @author zhw
 */
public interface SourceFactory {

    Source create(String sourceName, String type)
            throws FlumeException;

    Class<? extends Source> getClass(String type)
            throws FlumeException;
}