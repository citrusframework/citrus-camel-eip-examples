package com.example.eip.flags;

import dev.openfeature.sdk.Client;
import dev.openfeature.sdk.MutableContext;
import dev.openfeature.sdk.OpenFeatureAPI;
import dev.openfeature.contrib.providers.flagd.FlagdProvider;
import dev.openfeature.contrib.providers.flagd.FlagdOptions;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Thin wrapper over the OpenFeature client, registered in the Camel registry as
 * {@code featureFlags} so routes can call it from a processor.
 *
 * <p>Every lookup takes a default. That is not defensive boilerplate -- it is
 * the contract: if flagd is unreachable, the SDK returns the default rather
 * than throwing, and the route keeps running on the old behaviour. A feature
 * flag system that can take your application down with it is worse than no
 * feature flag system.
 */
@ApplicationScoped
@Named("featureFlags")
public class FeatureFlagEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagEvaluator.class);

    @ConfigProperty(name = "flagd.host", defaultValue = "localhost")
    String flagdHost;

    @ConfigProperty(name = "flagd.port", defaultValue = "8013")
    int flagdPort;

    @ConfigProperty(name = "flagd.provider.init", defaultValue = "true")
    boolean initProvider;

    private Client client;

    @PostConstruct
    void init() {
        OpenFeatureAPI api = OpenFeatureAPI.getInstance();
        if (initProvider) {
            FlagdOptions options = FlagdOptions.builder()
                .host(flagdHost)
                .port(flagdPort)
                .build();
            // setProvider, not setProviderAndWait. The provider establishes its
            // event stream to flagd in the background, and blocking startup on that
            // stream buys nothing here: every lookup below already carries a
            // default, so evaluations made before the stream is up simply get the
            // default rather than failing. Waiting only converts a slow flagd into
            // a failed startup.
            api.setProvider(new FlagdProvider(options));
            LOG.info("OpenFeature client created for flagd at {}:{} (connecting in background)",
                flagdHost, flagdPort);
        } else {
            LOG.info("Skipping FlagdProvider init — using pre-configured provider");
        }
        client = api.getClient("eip-shipping");
    }

    /** Boolean flag, defaulting to false when flagd cannot be reached. */
    public boolean isEnabled(String flagKey) {
        return client.getBooleanValue(flagKey, false);
    }

    /** String variant with evaluation context, for targeting and fractional rollout. */
    public String getVariant(String flagKey, String fallback, Map<String, Object> context) {
        MutableContext evalContext = new MutableContext();
        context.forEach((k, v) -> evalContext.add(k, String.valueOf(v)));
        return client.getStringValue(flagKey, fallback, evalContext);
    }
}
