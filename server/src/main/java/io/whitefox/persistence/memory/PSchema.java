package io.whitefox.persistence.memory;

import java.util.List;

public record PSchema(String name, List<PTable> tables, String share) {}
