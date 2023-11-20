package com.shahidul.csvprocessor.controller;

import com.shahidul.csvprocessor.model.ResponseMessage;
import com.shahidul.csvprocessor.service.CSVService;
import com.shahidul.csvprocessor.utils.CSVHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.util.Date;

@Controller
@Slf4j
@RequiredArgsConstructor
public class CsvController {
    private final CSVService csvService;

    @GetMapping("/")
    public String homePage(Model model){
        log.info("inside home controller");
        return "home";
    }

    @PostMapping("/upload")
    public ResponseEntity<ResponseMessage> uploadFile(@RequestParam("file") MultipartFile file) {
        String message = "";
        log.debug("Starts processing uploaded csv file ...");
        if (CSVHelper.hasCSVFormat(file)) {
            try {
                Long startTime = new Date().getTime();
                csvService.processCSVFile(file);
                Long endTime = new Date().getTime();
                message = "Uploaded the file successfully: " + file.getOriginalFilename() + " in " + (endTime - startTime) + " ms.";
                return ResponseEntity.status(HttpStatus.OK).body(new ResponseMessage(message));
            } catch (Exception e) {
                message = "Failed to process the uploaded csv file: " + file.getOriginalFilename() + "!";
                log.error("Error during processing csv file {}", e.getMessage());
                e.printStackTrace();
                return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(new ResponseMessage(message));
            }
        }
        message = "Please upload a csv file!";
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ResponseMessage(message));
    }
}
