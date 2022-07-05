package com.example.batchprocessing;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

// tag::setup[]
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	// end::setup[]

	@Autowired
	public SPJobExecutionListener SPJobExecutionListener;

	@Bean
	public S3Client s3Client() {
		Region region = Region.US_EAST_2;
		String access_key_id = "AKIAVW6LG7TJDDJDXGWH";
		String secret_access_key = "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";

		AwsBasicCredentials awsCreds = AwsBasicCredentials.create(access_key_id, secret_access_key);

		S3Client s3 = S3Client.builder().region(region).credentialsProvider(StaticCredentialsProvider.create(awsCreds))
				.build();
		return s3;
	}

	@Bean
	public PersonItemReader itemReader() {
		return new PersonItemReader(s3Client());
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public SPItemWriteListener spItemWriteListener() {
		return new SPItemWriteListener(s3Client());
	}

	@Bean
	public JdbcBatchItemWriter<PersonData> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<PersonData>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO people (first_name, last_name) VALUES (:person.firstName, :person.lastName)")
				.dataSource(dataSource).build();
	}
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("importUserJob").incrementer(new RunIdIncrementer())
				// .listener(SPJobExecutionListener)
				.listener(listener).flow(step1).end().build();
	}

	@Bean
	public Step step1(JdbcBatchItemWriter<PersonData> writer) {
		return stepBuilderFactory.get("step1").<CSVData, PersonData>chunk(10).listener(spItemWriteListener())
				.reader(itemReader()).processor(processor()).writer(writer).build();
	}
	// end::jobstep[]
}
