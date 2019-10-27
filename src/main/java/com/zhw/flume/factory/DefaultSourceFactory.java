
package com.zhw.flume.factory;

import com.google.common.base.Preconditions;
import com.zhw.flume.conf.configuration.SourceType;
import com.zhw.flume.source.Source;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * @author zhw
 */
public class DefaultSourceFactory implements SourceFactory {

    private static final Logger logger = LoggerFactory
            .getLogger(DefaultSourceFactory.class);

    @Override
    public Source create(String name, String type) throws FlumeException {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(type, "type");
        logger.info("Creating instance of source {}, type {}", name, type);
        Class<? extends Source> sourceClass = getClass(type);
        try {
            Source source = sourceClass.newInstance();
            source.setName(name);
            return source;
        } catch (Exception ex) {
            throw new FlumeException("Unable to create source: " + name
                    + ", type: " + type + ", class: " + sourceClass.getName(), ex);
        }
    }
    @SuppressWarnings("unchecked")
    @Override
    public Class<? extends Source> getClass(String type) throws FlumeException {
        String sourceClassName = type;
        SourceType srcType = SourceType.OTHER;
        try {
            srcType = SourceType.valueOf(type.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException ex) {
            logger.debug("Source type {} is a custom type", type);
        }
        if (!srcType.equals(SourceType.OTHER)) {
            sourceClassName = srcType.getSourceClassName();
        }
        try {
            return (Class<? extends Source>) Class.forName(sourceClassName);
        } catch (Exception ex) {
            throw new FlumeException("Unable to load source type: " + type
                    + ", class: " + sourceClassName, ex);
        }
    }
}