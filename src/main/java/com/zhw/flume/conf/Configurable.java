
package com.zhw.flume.conf;

import org.apache.flume.Context;

/**
 * @author zhw
 */
public interface Configurable {

    void configure(Context context);
}