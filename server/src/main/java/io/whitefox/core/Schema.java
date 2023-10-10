package io.whitefox.core;

import java.util.List;

public record Schema(String name, List<Table> tables, String share) {}
