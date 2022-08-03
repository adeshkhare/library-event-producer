package com.learnkafka.libraryeventproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventproducer.domain.LibraryEventType;
import com.learnkafka.libraryeventproducer.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent1")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        // invoke kafka producer

        //libraryEventProducer.sendLibtraryEvent(libraryEvent);
        SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSync(libraryEvent);
        log.info("SendResult is {} ", sendResult.toString());

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);

    }


    //PUT
    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        libraryEvent.setEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventSync(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


}
