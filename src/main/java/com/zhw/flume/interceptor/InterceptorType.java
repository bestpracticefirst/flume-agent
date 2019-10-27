
package com.zhw.flume.interceptor;


/**
 * @author zhw
 */
public enum InterceptorType {

    QPS(QpsInterceptor.Builder.class);

    private final Class<? extends Interceptor.Builder> builderClass;

    InterceptorType(Class<? extends Interceptor.Builder> builderClass) {
        this.builderClass = builderClass;
    }

    public Class<? extends Interceptor.Builder> getBuilderClass() {
        return builderClass;
    }
}
