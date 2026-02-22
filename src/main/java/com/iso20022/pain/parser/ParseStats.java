package com.iso20022.pain.parser;

/** Lightweight summary returned by the streaming parse path. */
public record ParseStats(long messageRows, long remittanceRows, long transactionRows) {}
