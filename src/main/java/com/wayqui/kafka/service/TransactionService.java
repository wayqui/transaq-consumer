package com.wayqui.kafka.service;

import com.wayqui.kafka.dto.TransactionDto;

public interface TransactionService {

    void insertTransaction(TransactionDto transactionDto);
}
