package com.shahidul.csvprocessor.utils;

import com.shahidul.csvprocessor.model.Person;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.validator.routines.EmailValidator;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class CSVHelper {
    public static String TYPE = "text/csv";
    public static List<String> csvHeaders = Arrays.asList("firstname", "lastname", "phone", "email", "city", "zip");

    public static boolean hasCSVFormat(MultipartFile file) {
        return TYPE.equals(file.getContentType());
    }

    public static Iterable<CSVRecord> csvFileToCsvRecords(InputStream is) {
        try (BufferedReader fileReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(fileReader,
                     CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            return csvParser.getRecords();
        } catch (IOException e) {
            throw new RuntimeException("fail to parse CSV file: " + e.getMessage());
        }
    }

    public static void writeToCSV(CSVPrinter printer, CSVRecord csvRecord) {
        try {
            List<String> row = new ArrayList<>();
            for (int i = 0; i < csvRecord.size(); i++) {
                row.add(csvRecord.get(i));
            }
            printer.printRecord(row);
        } catch (IOException e) {
            log.error("Error during printing to csv file {}", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void writeToCSV(CSVPrinter printer, List<String> csvRecord) {
        try {
            printer.printRecord(csvRecord);
        } catch (IOException e) {
            log.error("Error during printing to csv file {}", e.getMessage());
            e.printStackTrace();
        }
    }
}
