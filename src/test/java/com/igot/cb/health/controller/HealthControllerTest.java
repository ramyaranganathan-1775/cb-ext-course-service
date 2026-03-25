package com.igot.cb.health.controller;


import com.igot.cb.health.service.HealthService;
import com.igot.cb.model.ApiResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HealthControllerTest {

    @Mock
    private HealthService healthService;

    @InjectMocks
    private HealthController healthController;

    @Test
    void testHealthCheck_success() throws Exception {
        // Arrange
        ApiResponse mockResponse = new ApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);

        when(healthService.checkHealthStatus(anyString()))
                .thenReturn(mockResponse);

        // Act
        ResponseEntity<ApiResponse> response = healthController.healthCheck();

        // Assert
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());

        // Verify UUID was passed
        verify(healthService).checkHealthStatus(anyString());
    }


}