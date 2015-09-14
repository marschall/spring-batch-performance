package com.github.marschall.spring.batch.performance;

import javax.annotation.PostConstruct;

import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;


public class TestBatchConfigurer implements BatchConfigurer {

  private JobExplorer jobExplorer;
  private SimpleJobLauncher jobLauncher;
  private JobRepository jobRepository;
  private PlatformTransactionManager transactionManager;
  
  @PostConstruct
  public void initialize() {
    try {
      this.transactionManager = new ResourcelessTransactionManager();
      MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(this.transactionManager);
      jobRepositoryFactory.afterPropertiesSet();
      this.jobRepository = jobRepositoryFactory.getObject();
      
      MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean(jobRepositoryFactory);
      jobExplorer = jobExplorerFactory.getObject();
      
      this.jobLauncher = new SimpleJobLauncher();
      this.jobLauncher.setJobRepository(jobRepository);
      this.jobLauncher.afterPropertiesSet();
    } catch (Exception e) {
      throw new BatchConfigurationException(e);
    }
  }

  @Override
  public JobExplorer getJobExplorer() {
    return this.jobExplorer;
  }

  @Override
  public JobLauncher getJobLauncher() {
    return this.jobLauncher;
  }

  @Override
  public JobRepository getJobRepository() {
    return this.jobRepository;
  }

  @Override
  public PlatformTransactionManager getTransactionManager() {
    return this.transactionManager;
  }

}
