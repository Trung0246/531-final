package com.datasetviz.controller;

import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.schema.DataFetchingEnvironment;
import org.springframework.graphql.execution.DataFetcherExceptionResolverAdapter;
import org.springframework.graphql.execution.ErrorType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.NoSuchElementException;

@Component
public class GraphqlExceptionHandler extends DataFetcherExceptionResolverAdapter {

    @Override
    protected GraphQLError resolveToSingleError(Throwable exception, DataFetchingEnvironment environment) {
        if (exception instanceof IllegalArgumentException) {
            return error(environment, ErrorType.BAD_REQUEST, exception.getMessage());
        }
        if (exception instanceof NoSuchElementException) {
            return error(environment, ErrorType.NOT_FOUND, exception.getMessage());
        }
        if (exception instanceof IOException) {
            return error(environment, ErrorType.INTERNAL_ERROR, exception.getMessage());
        }
        return null;
    }

    private GraphQLError error(DataFetchingEnvironment environment, ErrorType errorType, String message) {
        return GraphqlErrorBuilder.newError(environment)
                .errorType(errorType)
                .message(message == null || message.isBlank() ? errorType.name() : message)
                .build();
    }
}
