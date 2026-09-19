package com.example.jms.web;

import com.example.jms.service.DynamicAsyncListenerService;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.util.logging.Logger;

/**
 * Ensures background JMS resources are stopped when the WAR is undeployed in JEUS.
 */
@WebListener
public class AppContextListener implements ServletContextListener {

    private static final Logger LOGGER = Logger.getLogger(AppContextListener.class.getName());

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        LOGGER.info("JEUS JMS Test Application initialized.");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        LOGGER.info("JEUS JMS Test Application stopping. Cleaning up JMS resources...");
        DynamicAsyncListenerService.getInstance().stop();
    }
}
