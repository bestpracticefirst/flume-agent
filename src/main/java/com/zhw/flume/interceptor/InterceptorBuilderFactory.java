
package com.zhw.flume.interceptor;

import com.zhw.flume.interceptor.Interceptor.Builder;
/**
 * @author zhw
 */
public class InterceptorBuilderFactory {

    /**
     * Instantiate specified class, either alias or fully-qualified class name.
     */
    public static Builder newInstance(String name)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException {

        Class<? extends Builder> clazz = (Class<? extends Builder>) Class.forName(name);

        return clazz.newInstance();
    }
}