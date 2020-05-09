package com.wayqui.kafka.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Transaction {

    @Id
    private String Id;
    private String reference;
    private String iban;
    private Instant date;
    private Double amount;
    private Double fee;
    private String description;
}
