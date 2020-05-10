package com.wayqui.kafka.mapper;

import com.wayqui.kafka.dto.TransactionDto;
import com.wayqui.kafka.entity.Transaction;
import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

import java.util.List;

@Mapper
public interface TransactionMapper {

    TransactionMapper INSTANCE = Mappers.getMapper( TransactionMapper.class );

    Transaction dtoToEntity(TransactionDto transaction);
    TransactionDto entityToDto(Transaction result);

    List<TransactionDto> entitiesToDtos(List<Transaction> result);
    List<Transaction> dtosToEntities(List<TransactionDto> result);
}
