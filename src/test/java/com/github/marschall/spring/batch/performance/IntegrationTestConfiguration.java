package com.github.marschall.spring.batch.performance;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@EnableBatchProcessing
public class IntegrationTestConfiguration {
  
  @Autowired
  private JobBuilderFactory jobBuilderFactory;
  
  @Autowired
  private StepBuilderFactory stepBuilderFactory;
  
  @Bean
  public JobLauncherTestUtils jobLauncherTestUtils() {
    return new JobLauncherTestUtils();
  }
  
  @Bean
  public TestBatchConfigurer testBatchConfigurer() {
    return new TestBatchConfigurer();
  } 
  
  @Bean
  public Job job() {
    return this.jobBuilderFactory.get("job")
        .incrementer(new RunIdIncrementer())
        .start(this.step())
        .build();
  }
  
  @Bean
  public Step step() {
    StreamingFlatFileWriter<String> writer = new StreamingFlatFileWriter<>();
    return stepBuilderFactory.get("step").<String, String>chunk(10)
        .reader(new ListItemReader<>(items(100)))
        .writer(writer)
        .build();
  }
  
  private List<String> items(int count) {
    List<String> items = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      items.add(Integer.toString(i));
    }
    return items;
  }

}
