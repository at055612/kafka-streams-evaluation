package kafkastreamsevaluation.main;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Class for checking logback is working correctly as IntelliJ 2017.3 is not copying
 * src/main/resources to out/production/resources so the logback configuration is
 * not available on the classpath
 */
public class Logback {

    private static final Logger LOGGER = LoggerFactory.getLogger(Logback.class);

    public static void main(String[] args) {

        // assume SLF4J is bound to logback in the current environment
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        // print logback's internal status
        StatusPrinter.print(lc);

        LOGGER.debug("My debug");
        LOGGER.info("My info");

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            System.out.println(url.getFile());
        }
    }
}
