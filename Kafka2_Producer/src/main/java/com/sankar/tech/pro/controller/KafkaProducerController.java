package com.sankar.tech.pro.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sankar.tech.pro.dto.Employee;

@RestController
public class KafkaProducerController {
	
	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;
	
	private static final String TOPIC = "EmployeeTopic";
	
	@PostMapping("/produce")
	public ResponseEntity<String> postModelToKafka(@RequestBody Employee employee) throws JsonProcessingException, InterruptedException, ExecutionException
	{
		/*ObjectMapper mapper = new ObjectMapper();
		String writeValueAsString = mapper.writeValueAsString(employee);*/
		
		ListenableFuture<SendResult<String, Object>> send = kafkaTemplate.send(TOPIC, employee);
		
		return new ResponseEntity(send.get().getProducerRecord().value(), HttpStatus.OK);
	
	}
	
	@GetMapping("/produce/{message}")
	public ResponseEntity sendMessage(@PathVariable String message)
	{
		kafkaTemplate.send(TOPIC, message);
		return new ResponseEntity<>("Done : "+message,HttpStatus.OK);
	}

}
