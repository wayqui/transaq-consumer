package com.wayqui.kafka.dao;

import com.wayqui.kafka.entity.Transaction;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TransactionRepository extends MongoRepository<Transaction, String> {

}
