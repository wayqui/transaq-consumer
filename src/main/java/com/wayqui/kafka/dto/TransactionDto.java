package com.wayqui.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class TransactionDto {
    private String Id;
    private String reference;
    private String iban;
    private Instant date;
    private Double amount;
    private Double fee;
    private String description;
}
