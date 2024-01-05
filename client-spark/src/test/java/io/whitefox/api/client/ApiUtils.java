package io.whitefox.api.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.whitefox.api.utils.ApiClient;
import io.whitefox.api.utils.ApiException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Supplier;

public class ApiUtils {
    /**
     * Returns the result of the call of the first argument (f) unless it throws an ApiException with HTTP status code 409,
     * in that case, returns the result of the call of the second argument (defaultValue).
     * If defaultValue is not dynamic, you can use also {@link ApiUtils#recoverConflict recoverConflinct}.
     */
    public static <T> T recoverConflictLazy(Supplier<T> f, Supplier<T> defaultValue) {
        try {
            return f.get();
        } catch (ApiException e) {
            if (e.getCode() == 409) {
                return defaultValue.get();
            } else {
                throw e;
            }
        }
    }

    /**
     * Returns the result of the call of the first argument (f) unless it throws an ApiException with HTTP status code 409,
     * in that case, returns the second argument (defaultValue).
     * If defaultValue is dynamic, you can use also {@link ApiUtils#recoverConflictLazy recoverConflictLazy}.
     */
    public static <T> T recoverConflict(Supplier<T> f, T defaultValue) {
        return recoverConflictLazy(f, new Supplier<T>() {
            @Override
            public T get() {
                return defaultValue;
            }
        });
    }

    /**
     * Calls the first argument (f), if the call throws an ApiException with HTTP status code 409 will swallow the exception.
     */
    public static <T> void ignoreConflict(Supplier<T> f) {
        recoverConflict(f, null);
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String ENDPOINT_FIELD_NAME = "endpoint";
    public static final String BEARER_TOKEN_FIELD_NAME = "bearerToken";

    /**
     * Reads a resource named as the parameter, parses it following
     * <a href="https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format">delta sharing specification</a>
     * and configures an {@link ApiClient ApiClient} accordingly.
     */
    public static ApiClient configureApiClientFromResource(String resourceName) {
        try (InputStream is = ApiUtils.class.getClassLoader().getResourceAsStream(resourceName)) {
            return configureClientInternal(objectMapper.reader().readTree(is));
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot read %s", resourceName), e);
        } catch (NullPointerException e) {
            throw new RuntimeException(String.format("Cannot find resource %s", resourceName), e);
        }
    }

    /**
     * Reads a local file named as the parameter, parses it following
     * <a href="https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format">delta sharing specification</a>
     * and configures an {@link ApiClient ApiClient} accordingly.
     */
    public static ApiClient configureClientFromFile(File file) {
        try (InputStream is = new FileInputStream(file)) {
            return configureClientInternal(objectMapper.reader().readTree(is));
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot read %s", file), e);
        }
    }

    private static ApiClient configureClientInternal(JsonNode conf) {
        var endpointText = getRequiredField(conf, ENDPOINT_FIELD_NAME).asText();
        var token = getRequiredField(conf, BEARER_TOKEN_FIELD_NAME).asText();
        try {
            var endpoint = new URI(endpointText);
            var apiClient = new ApiClient();
            apiClient.setHost(endpoint.getHost());
            apiClient.setPort(endpoint.getPort());
            apiClient.setScheme(endpoint.getScheme());
            apiClient.setRequestInterceptor(
                    builder -> builder.header("Authorization", String.format("Bearer %s", token)));
            return apiClient;
        } catch (URISyntaxException u) {
            throw new RuntimeException(String.format("Invalid endpoint syntax %s", endpointText), u);
        }
    }

    private static JsonNode getRequiredField(JsonNode node, String fieldName) {
        if (node.has(fieldName)) {
            return node.get(fieldName);
        } else {
            throw new RuntimeException(
                    String.format("Cannot find required field %s in %s", fieldName, node));
        }
    }
}
