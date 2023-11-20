package com.shahidul.csvprocessor.service;

import com.shahidul.csvprocessor.model.Person;
import com.shahidul.csvprocessor.utils.CSVHelper;
import com.shahidul.csvprocessor.utils.ListUtils;
import com.shahidul.csvprocessor.utils.ValidationUtils;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.Table;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
//import org.hibernate.mapping.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.validator.routines.EmailValidator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class CSVService {
//    private final PersonRepository personRepository;
    private final HikariDataSource hikariDataSource;

    @Value("${spring.jpa.properties.hibernate.jdbc.batch_size}")
    private int batchSize;

    public void processCSVFile(MultipartFile file) {
        try {
            log.debug("Processing csv file...");
            Iterable<CSVRecord> csvRecords = CSVHelper.csvFileToCsvRecords(file.getInputStream());

            CSVPrinter validPrinter = new CSVPrinter(new FileWriter("valid_data_" + new Date().getTime() + ".csv"), CSVFormat.DEFAULT);
            CSVPrinter invalidPrinter = new CSVPrinter(new FileWriter("invalid_data_" + new Date().getTime() + ".csv"), CSVFormat.DEFAULT);

            validPrinter.printRecord(CSVHelper.csvHeaders);
            invalidPrinter.printRecord(CSVHelper.csvHeaders);

            List<Person> validPersonList = new ArrayList<>();

            for (CSVRecord csvRecord : csvRecords) {
                String phone = null, email = null;
                if (csvRecord.size() >= 6) {
                    phone = csvRecord.get(2);
                    email = csvRecord.get(3);
                }
                if (email != null && phone != null &&
                        EmailValidator.getInstance().isValid(email) && ValidationUtils.isValidPhone(phone)) {
                    Person person = new Person(
                            null,
                            csvRecord.get(0), // firstname
                            csvRecord.get(1), // lastname
                            csvRecord.get(2), // phone
                            csvRecord.get(3), // email
                            csvRecord.get(4), // city
                            Long.parseLong(csvRecord.get(5)) // zip
                    );
                    validPersonList.add(person);
                    CSVHelper.writeToCSV(validPrinter, csvRecord);
                } else {
                    CSVHelper.writeToCSV(invalidPrinter, csvRecord);
                }
            }
            log.debug("Ends preparing valid person data");
            validPrinter.flush();
            validPrinter.close();

            invalidPrinter.flush();
            invalidPrinter.close();

            saveAllJdbcBatchCallable(validPersonList);
        } catch (IOException e) {
            log.error("Error occurred {}", e.getMessage());
            throw new RuntimeException("fail to store csv data: " + e.getMessage());
        }
    }

    public void saveAllJdbcBatchCallable(List<Person> productData){

        log.debug("Inserting using jdbc batch and threading");
        Long startTime = new Date().getTime();
        log.debug("Before executorService starts {}", startTime);
        ExecutorService executorService = Executors.newFixedThreadPool(hikariDataSource.getMaximumPoolSize());

        List<List<Person>> listOfBookSub = ListUtils.createSubList(productData, batchSize);

        List<Callable<Void>> callables = listOfBookSub.stream().map(sublist ->
                (Callable<Void>) () -> {
                    saveAllJdbcBatch(sublist);
                    return null;
                }).collect(Collectors.toList());
        try {
            executorService.invokeAll(callables);
            Long endTime = new Date().getTime();
            log.debug("After executorService ends {}", endTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void saveAllJdbcBatch(List<Person> personData) {
        log.debug("insert using jdbc batch");
        String sql = String.format(
                "INSERT INTO %s (firstname, lastname, phone, email, city, zip) " +
                        "VALUES (?, ?, ?, ?, ?, ?)",
                Person.class.getAnnotation(Table.class).name()
        );
        try (Connection connection = hikariDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            int counter = 0;
            for (Person person : personData) {
                statement.clearParameters();
                statement.setString(1, person.getFirstname());
                statement.setString(2, person.getLastname());
                statement.setString(3, person.getPhone());
                statement.setString(4, person.getEmail());
                statement.setString(5, person.getCity());
                statement.setLong(6, person.getZip());
                statement.addBatch();
                if ((counter + 1) % batchSize == 0 || (counter + 1) == personData.size()) {
                    statement.executeBatch();
                    statement.clearBatch();
                }
                counter++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void prepareCSVData() {
        try {
            CSVPrinter inputPrinter = new CSVPrinter(new FileWriter("input_data_" + new Date().getTime() + ".csv"), CSVFormat.DEFAULT);
            inputPrinter.printRecord(CSVHelper.csvHeaders);

            for (int i = 0; i < 1000000; i++) {
                List<String> data = Arrays.asList(
                        "firstName_" + i,
                        "lastName_" + i,
                        "(123)-456-7890",
                        "test@gmail.com",
                        "dhaka",
                        String.valueOf(i)
                );
                CSVHelper.writeToCSV(inputPrinter, data);
            }
            inputPrinter.flush();
            inputPrinter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
