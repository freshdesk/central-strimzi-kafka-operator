/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import io.strimzi.operator.common.model.NodeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Collects and writes the configuration collected in the init container
 */
public class InitWriter {
    private static final Logger LOGGER = LogManager.getLogger(InitWriter.class);

    private final KubernetesClient client;
    private final InitWriterConfig config;

    protected final static String FILE_RACK_ID = "rack.id";
    protected final static String FILE_EXTERNAL_ADDRESS = "external.address";
    protected final static String FILE_JAAS_CONF = "jaas.conf";

    /**
     * Constructs the InitWriter
     *
     * @param client    Kubernetes client
     * @param config    InitWriter configuration
     */
    public InitWriter(KubernetesClient client, InitWriterConfig config) {
        this.client = client;
        this.config = config;
    }

    /**
     * Write the rack-id
     *
     * @return if the operation was executed successfully
     */
    public boolean writeRack() {

        Map<String, String> nodeLabels = client.nodes().withName(config.getNodeName()).get().getMetadata().getLabels();
        LOGGER.info("NodeLabels = {}", nodeLabels);
        String rackId = nodeLabels.get(config.getRackTopologyKey());
        LOGGER.info("Rack: {} = {}", config.getRackTopologyKey(), rackId);

        if (rackId == null) {
            LOGGER.error("Node {} doesn't have the label {} for getting the rackid",
                    config.getNodeName(), config.getRackTopologyKey());
            return false;
        }

        return write(FILE_RACK_ID, rackId);
    }

    /**
     * Write the external address of this node
     *
     * @return if the operation was executed successfully
     */
    public boolean writeExternalAddress() {
        List<NodeAddress> addresses = client.nodes().withName(config.getNodeName()).get().getStatus().getAddresses();
        StringBuilder externalAddresses = new StringBuilder();

        String address = NodeUtils.findAddress(addresses, null);

        if (address == null) {
            LOGGER.error("External address not found");
            return false;
        } else  {
            LOGGER.info("Default External address found {}", address);
            externalAddresses.append(externalAddressExport(null, address));
        }

        for (NodeAddressType type : NodeAddressType.values())   {
            address = NodeUtils.findAddress(addresses, type);
            LOGGER.info("External {} address found {}", type.toValue(), address);
            externalAddresses.append(externalAddressExport(type, address));
        }

        return write(FILE_EXTERNAL_ADDRESS, externalAddresses.toString());
    }

    /**
     * Formats address type and address into shell export command for environment variable
     *
     * @param type      Type of the address. Use null for default address
     * @param address   Address for given type
     * @return          String with the shell command
     */
    private String externalAddressExport(NodeAddressType type, String address) {
        String envVar;

        if (type != null) {
            envVar = String.format("STRIMZI_NODEPORT_%s_ADDRESS", type.toValue().toUpperCase(Locale.ENGLISH));
        } else {
            envVar = "STRIMZI_NODEPORT_DEFAULT_ADDRESS";
        }

        return String.format("export %s=%s", envVar, address) + System.lineSeparator();
    }

    /**
     * Write the fwss user secrets to jaas.conf
     *
     * @param namespace   The namespace in which kafka is running
     * @param secretList   List of secrets in the namespace
     * @return if the operation was executed successfully
     */
    public boolean writeFwssSecretsToJaasConf(String namespace, SecretList secretList) {

        if (secretList.getItems().isEmpty()) {
            LOGGER.error("SecretList is empty");
            return false;
        }
        List<Secret> secrets = secretList.getItems();
        List<Secret> filteredSecrets = secrets.stream()
                .filter(secret -> secret.getMetadata().getName().startsWith(config.getFwssSecretPrefix()))
                .toList();
        if (filteredSecrets.isEmpty()) {
            LOGGER.error("No secrets starting with '{}' found.", config.getFwssSecretPrefix());
            return false;
        }

        String adminSecretPrefix = config.getFwssSecretPrefix() + "-admin";

        List<Secret> filteredAdminSecrets = secrets.stream()
                .filter(secret -> secret.getMetadata().getName().startsWith(adminSecretPrefix))
                .toList();
        if (filteredAdminSecrets.isEmpty()) {
            LOGGER.error("No admin secrets starting with '{}' found.", adminSecretPrefix);
            return false;
        } else if (filteredAdminSecrets.size() > 1) {
            LOGGER.error("More than one admin secrets starting with '{}' found", adminSecretPrefix);
            return false;
        }

        Map.Entry<String, String> adminNameAndSecret = filteredAdminSecrets.get(0).getData().entrySet().iterator().next();
        String adminUser = adminNameAndSecret.getKey();
        String adminPassword = new String(java.util.Base64.getDecoder().decode(adminNameAndSecret.getValue())).trim();
        StringBuilder jaasConfig = new StringBuilder();
        jaasConfig.append("KafkaServer {\n");
        jaasConfig.append("  org.apache.kafka.common.security.plain.PlainLoginModule required\n");
        jaasConfig.append("  username").append("=\"").append(adminUser).append("\"\n");
        jaasConfig.append("  password").append("=\"").append(adminPassword).append("\"\n");

        for (Secret secret : filteredSecrets) {
            Map<String, String> data = secret.getData();
            for (Map.Entry<String, String> entry : data.entrySet()) {
                String key = entry.getKey();
                String value = new String(java.util.Base64.getDecoder().decode(entry.getValue())).trim();
                jaasConfig.append("  user_").append(key).append("=\"").append(value).append("\"\n");
            }
        }
        // Replace the last newline character jaasConfig with ";"
        jaasConfig.setCharAt(jaasConfig.length() - 1, ';');
        jaasConfig.append("\n};");

        return write(FILE_JAAS_CONF, jaasConfig.toString());
    }

    /**
     * Write provided information into a file
     *
     * @param file          Target file
     * @param information   Information to be written
     * @return              true if write succeeded, false otherwise
     */
    private boolean write(String file, String information) {
        boolean isWritten;

        try (PrintWriter writer = new PrintWriter(config.getInitFolder() + "/" + file, StandardCharsets.UTF_8)) {
            writer.write(information);

            if (writer.checkError())    {
                LOGGER.error("Failed to write the information {} to file {}", information, file);
                isWritten = false;
            } else {
                if (file.equals(FILE_JAAS_CONF)) {
                    LOGGER.info("Jaas information string of length {} written successfully to file {}", information.length(), file);
                } else {
                    LOGGER.info("Information {} written successfully to file {}", information, file);
                }
                isWritten = true;
            }
        } catch (IOException e) {
            LOGGER.error("Error writing the information {} to file {}", information, file, e);
            isWritten = false;
        }

        return isWritten;
    }
}
