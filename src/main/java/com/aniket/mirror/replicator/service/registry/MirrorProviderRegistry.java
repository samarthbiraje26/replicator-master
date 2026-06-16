package com.aniket.mirror.replicator.service.registry;

import com.aniket.mirror.replicator.constants.ProviderType;
import com.aniket.mirror.replicator.service.mirror.MirrorProviderService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class MirrorProviderRegistry {

  private final Map<ProviderType, MirrorProviderService> providers;

  public MirrorProviderRegistry(List<MirrorProviderService> list) {
    this.providers = list.stream()
        .collect(Collectors.toMap(
            MirrorProviderService::getType,
            Function.identity()
        ));
  }

  public MirrorProviderService get(ProviderType type) {
    return Optional.ofNullable(providers.get(type))
        .orElseThrow(() ->
            new IllegalStateException(
                "No mirror provider for " + type));
  }
}
