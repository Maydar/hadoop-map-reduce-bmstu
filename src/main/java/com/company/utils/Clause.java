package com.company.utils;

/**
 * @author maydar
 * @since 18.06.17
 */


public class Clause {
    private String fieldName;
    private Operation operation;
    private String value;

    public Clause() {}

    public Clause(String fieldName, Operation operation, String value) {
        this.fieldName = fieldName;
        this.operation = operation;
        this.value = value;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
