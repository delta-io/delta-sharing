package io.whitefox.persistence.memory;

import java.util.Map;

public record PShare(String name, String id, Map<String, PSchema> schemas) {}
