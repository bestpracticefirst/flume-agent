
package com.zhw.flume.conf.configuration;

import com.zhw.flume.conf.ComponentWithClassName;

/**
 * @author zhw
 */
public enum SinkType implements ComponentWithClassName {
    OTHER(null);

    private final String sinkClassName;

    private SinkType(String sinkClassName) {
        this.sinkClassName = sinkClassName;
    }

    @Deprecated
    public String getSinkClassName() {
        return sinkClassName;
    }


    @Override
    public String getClassName() {
        return sinkClassName;
    }
}