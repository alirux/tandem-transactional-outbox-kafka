package com.codingful.tandem.sample.jpa;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Standard Boot entry point; demo infrastructure and narration live in separate components. */
@SpringBootApplication
public class JpaSampleApplication {
    public static void main(String[] args) {
        SpringApplication.run(JpaSampleApplication.class, args);
    }
}
