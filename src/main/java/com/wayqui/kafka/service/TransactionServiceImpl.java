package com.wayqui.kafka.service;

import com.wayqui.kafka.dao.TransactionRepository;
import com.wayqui.kafka.dto.TransactionDto;
import com.wayqui.kafka.entity.Transaction;
import com.wayqui.kafka.mapper.TransactionMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TransactionServiceImpl implements TransactionService {

    @Autowired
    private TransactionRepository repository;

    @Override
    public void insertTransaction(TransactionDto transactionDto) {
        Transaction entity = TransactionMapper.INSTANCE.dtoToEntity(transactionDto);
        repository.save(entity);
    }
}
