
package com.zhw.flume.source;

import com.zhw.flume.lifecycle.LifecycleAware;

/**
 * @author zhw
 */
public abstract class SourceRunner implements LifecycleAware {

    private Source source;

    public static SourceRunner forSource(Source source) {
        SourceRunner runner = null;

        if (source instanceof PollableSource) {
            runner = new PollableSourceRunner();
            ((PollableSourceRunner) runner).setSource((PollableSource) source);
        } else {
            throw new IllegalArgumentException("No known runner type for source "
                    + source);
        }

        return runner;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }
}