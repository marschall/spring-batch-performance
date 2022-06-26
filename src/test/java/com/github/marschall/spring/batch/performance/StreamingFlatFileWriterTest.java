package com.github.marschall.spring.batch.performance;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;


@SpringJUnitConfig(IntegrationTestConfiguration.class)
public class StreamingFlatFileWriterTest {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  private FileSystem fileSystem;

  @Test
  public void test() throws Exception {
    JobParametersBuilder parametersBuilder = new JobParametersBuilder();
    JobExecution job = this.jobLauncherTestUtils.launchJob(parametersBuilder.toJobParameters());

    assertEquals(BatchStatus.COMPLETED, job.getStatus());
    assertEquals(IntegrationTestConfiguration.items(100), this.readLines());
  }

  private List<String> readLines() throws IOException {
    return Files.lines(this.fileSystem.getPath(IntegrationTestConfiguration.OUTPUT_FILE))
        .collect(Collectors.toList());
  }

}
