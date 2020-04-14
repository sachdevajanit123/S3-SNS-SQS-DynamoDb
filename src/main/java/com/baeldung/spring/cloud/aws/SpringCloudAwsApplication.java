package com.baeldung.spring.cloud.aws;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.XADataSourceAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class,XADataSourceAutoConfiguration.class })

public class SpringCloudAwsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringCloudAwsApplication.class, args);
    }
}
