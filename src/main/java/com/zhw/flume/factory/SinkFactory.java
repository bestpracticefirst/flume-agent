
package com.zhw.flume.factory;

import com.zhw.flume.sink.Sink;
import org.apache.flume.FlumeException;

/**
 * @author zhw
 */
public interface SinkFactory {

    Sink create(String name, String type)
            throws FlumeException;

    Class<? extends Sink> getClass(String type)
            throws FlumeException;
}