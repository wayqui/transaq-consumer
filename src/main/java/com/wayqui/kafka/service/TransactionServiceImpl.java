package com.wayqui.kafka.service;

import com.wayqui.kafka.dao.TransactionRepository;
import com.wayqui.kafka.dto.TransactionDto;
import com.wayqui.kafka.entity.Transaction;
import com.wayqui.kafka.mapper.TransactionMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class TransactionServiceImpl implements TransactionService {

    @Autowired
    private TransactionRepository repository;

    @Override
    public void insertTransaction(TransactionDto transactionDto) {
        if (transactionDto.getReference() == null) {
            log.error("onMessage -> A reference id must be informed");
            throw new RecoverableDataAccessException("A reference id must be informed");
        }
        if (transactionDto.getAmount() < 0) {
            log.error("onMessage -> A negative amount is not valid here");
            throw new IllegalArgumentException("A negative amount is not valid here");
        }
        Transaction entity = TransactionMapper.INSTANCE.dtoToEntity(transactionDto);
        repository.save(entity);
    }

    @Override
    public List<TransactionDto> findAllTransactions() {
        List<Transaction> result = repository.findAll();
        return TransactionMapper.INSTANCE.entitiesToDtos(result);
    }
}
