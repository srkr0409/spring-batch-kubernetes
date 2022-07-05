package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

public class PersonItemProcessor implements ItemProcessor<CSVData, PersonData> {

	private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

	@Override
	public PersonData process(CSVData item) throws Exception {
		String firstName = item.getData()[0].toUpperCase(); 
		String lastName = item.getData()[1].toUpperCase();
		Person transformedPerson = new Person(firstName, lastName);
		  
		  log.info("Converting into (" + transformedPerson + ")");
		  PersonData personData = new PersonData(transformedPerson, item.getFileName(), item.isLast());
		  return personData;
	}

}
