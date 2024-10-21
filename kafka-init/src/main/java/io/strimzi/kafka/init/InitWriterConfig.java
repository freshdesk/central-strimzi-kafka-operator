/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.strimzi.operator.common.config.ConfigParameter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.common.config.ConfigParameterParser.BOOLEAN;
import static io.strimzi.operator.common.config.ConfigParameterParser.NON_EMPTY_STRING;
import static io.strimzi.operator.common.config.ConfigParameterParser.STRING;

/**
 * Init Writer configuration
 */
public class InitWriterConfig {
    private static final Logger LOGGER = LogManager.getLogger(InitWriterConfig.class);
    private static final String NAMESPACE_FILE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
    private static final Map<String, ConfigParameter<?>> CONFIG_VALUES = new HashMap<>();
    /**
     * Folder where the rackid file is written
     */
    public static final ConfigParameter<String> INIT_FOLDER = new ConfigParameter<>("INIT_FOLDER", STRING, "/opt/kafka/init", CONFIG_VALUES);
    /**
     * Kubernetes cluster node name from which getting the rack related label
     */
    public static final ConfigParameter<String> NODE_NAME = new ConfigParameter<>("NODE_NAME", NON_EMPTY_STRING, CONFIG_VALUES);
    /**
     * Kubernetes cluster node label to use as topology key for rack definition
     */
    public static final ConfigParameter<String> RACK_TOPOLOGY_KEY = new ConfigParameter<>("RACK_TOPOLOGY_KEY", STRING, null, CONFIG_VALUES);
    /**
     * Whether external address should be acquired
     */
    public static final ConfigParameter<Boolean> EXTERNAL_ADDRESS = new ConfigParameter<>("EXTERNAL_ADDRESS", BOOLEAN, "false", CONFIG_VALUES);
    /**
     * The address type which should be preferred in the selection
     */
    public static final ConfigParameter<String> EXTERNAL_ADDRESS_TYPE = new ConfigParameter<>("EXTERNAL_ADDRESS_TYPE", STRING, null, CONFIG_VALUES);
    /**
     * FWSS secret prefix to filter secrets
     */
    public static final ConfigParameter<String> FWSS_SECRETS_PREFIX = new ConfigParameter<>("FWSS_SECRETS_PREFIX", STRING, "fwss", CONFIG_VALUES);
    /**
     * Authentication is of type sasl_scram_and_plain or others.
     */
    public static final ConfigParameter<String> AUTHENTICATION_IS_SASL_SCRAM_AND_PLAIN = new ConfigParameter<>("AUTHENTICATION_IS_SASL_SCRAM_AND_PLAIN", STRING, "false", CONFIG_VALUES);
    private final Map<String, Object> map;

    /**
     * @return Set of configuration key/names
     */
    public static Set<String> keyNames() {
        return Collections.unmodifiableSet(CONFIG_VALUES.keySet());
    }

    private InitWriterConfig(Map<String, Object> map) {
        this.map = map;
    }
    /**
     * Load configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return Rack Writer configuration instance
     */
    static InitWriterConfig fromMap(Map<String, String> map) {
        Map<String, String> envMap = new HashMap<>(map);
        envMap.keySet().retainAll(InitWriterConfig.keyNames());

        Map<String, Object> generatedMap = ConfigParameter.define(envMap, CONFIG_VALUES);

        return new InitWriterConfig(generatedMap);
    }

    /**
     * Gets the configuration value corresponding to the key
     * @param <T>      Type of value
     * @param value    Instance of Config Parameter class
     * @return         Configuration value w.r.t to the key
     */
    @SuppressWarnings("unchecked")
    public <T> T get(ConfigParameter<T> value) {
        return (T) this.map.get(value.key());
    }

    /**
     * @return Kubernetes cluster node name from which getting the rack related label
     */
    public String getNodeName() {
        return get(NODE_NAME);
    }

    /**
     * @return the Kubernetes cluster node label to use as topology key for rack definition
     */
    public String getRackTopologyKey() {
        return get(RACK_TOPOLOGY_KEY);
    }

    /**
     * @return folder where the rackid file is written
     */
    public String getInitFolder() {
        return get(INIT_FOLDER);
    }

    /**
     * @return Return whether external address should be acquired
     */
    public boolean isExternalAddress() {
        return get(EXTERNAL_ADDRESS);
    }

    /**
     * @return The address type which should be preferred in the selection
     */
    public String getAddressType() {
        return get(EXTERNAL_ADDRESS_TYPE);
    }

    /**
     * @return FWSS secret prefix to filter secrets
     */
    public String getFwssSecretPrefix() {
        return get(FWSS_SECRETS_PREFIX);
    }

    /**
     * @return if Authentication is of type sasl_scram_and_plain or others.
     */
    public boolean getIfAuthenticationIsSaslScramAndPlain() {
        String result = get(AUTHENTICATION_IS_SASL_SCRAM_AND_PLAIN);
        return result.equals("true");
    }

    /**
     * Reads the namespace from the file and returns it as a string.
     *
     * @return The namespace of the pod.
     */
    public String getNamespace() {
        String namespace;
        try {
            namespace = new String(Files.readAllBytes(Paths.get(NAMESPACE_FILE_PATH))).trim();
        } catch (IOException e) {
            namespace = "";
            LOGGER.error("Reading namespace file failed", e);
        }
        return namespace;
    }

    @Override
    public String toString() {
        return "InitWriterConfig(" +
                "nodeName=" + getNodeName() +
                ",rackTopologyKey=" + getRackTopologyKey() +
                ",externalAddress=" + isExternalAddress() +
                ",initFolder=" + getInitFolder() +
                ",addressType=" + getAddressType() +
                ",fwssSecretPrefix=" + getFwssSecretPrefix() +
                ",authenticationIsSaslScramAndPlain=" + getIfAuthenticationIsSaslScramAndPlain() +
                ",namespace=" + getNamespace() +
                ")";
    }
}
