package io.whitefox.core;

import java.util.Map;

public record Share(String name, String id, Map<String, Schema> schemas) {}
