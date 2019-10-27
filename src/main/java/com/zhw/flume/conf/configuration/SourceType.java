
package com.zhw.flume.conf.configuration;


import org.apache.flume.conf.ComponentWithClassName;

/**
 * @author zhw
 */
public enum SourceType implements ComponentWithClassName {

    OTHER(null),
    TAIL_DIR("com.zhw.flume.source.tail.TailDirSource");


    private final String sourceClassName;

    SourceType(String sourceClassName) {
        this.sourceClassName = sourceClassName;
    }

    @Deprecated
    public String getSourceClassName() {
        return sourceClassName;
    }

    @Override
    public String getClassName() {
        return sourceClassName;
    }

}
