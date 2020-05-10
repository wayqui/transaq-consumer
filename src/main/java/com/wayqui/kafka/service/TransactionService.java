package com.wayqui.kafka.service;

import com.wayqui.kafka.dto.TransactionDto;

import java.util.List;

public interface TransactionService {

    void insertTransaction(TransactionDto transactionDto);

    List<TransactionDto> findAllTransactions();
}
