package com.datastream;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataValidator {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = Logger.getLogger(Consumer.class);

    public static JsonNode parseStringToJson(String message) throws IOException {
        return objectMapper.readTree(message);
    }

    public static String transformStringTopPrettyJsonString(String jsonString) {
        try {
            JsonNode jsonNode = parseStringToJson(jsonString);
            return jsonNode.toPrettyString();
        }
        catch (JsonParseException e) {
            logger.error("JsonParseException: Invalid JSON format - " + jsonString, e);
        } catch (JsonMappingException e) {
            logger.error("JsonMappingException: JSON mapping failed - " + jsonString, e);
        } catch (IOException e) {
            logger.error("IOException: Error reading JSON string - " + jsonString, e);
        }

        return null;
        
    }
}
