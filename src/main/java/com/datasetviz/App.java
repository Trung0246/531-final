package com.datasetviz;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.system.ApplicationHome;

import java.io.File;
import java.util.Map;

@SpringBootApplication
public class App {

    public static void main(String[] args) {
        createApplication().run(args);
    }

    static SpringApplication createApplication() {
        SpringApplication application = new SpringApplication(App.class);
        application.setDefaultProperties(Map.of(
                "spring.config.additional-location",
                getAdditionalConfigLocation()
        ));
        return application;
    }

    static File getApplicationDirectory() {
        return new ApplicationHome(App.class).getDir();
    }

    static String getAdditionalConfigLocation() {
        return "optional:file:" + getApplicationDirectory().getAbsolutePath() + "/";
    }
}
