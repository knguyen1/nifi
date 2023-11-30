/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.geneva;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BaseExecuteGeneva extends AbstractProcessor {

    static final AllowableValue USERNAME_PASSWORD_STRATEGY = new AllowableValue("username-password", "Username and Password", "Use your username and password as SSH credentials.  Note this is (usually) no the same username and password for your `runrep` utility.")
    static final AllowableValue IDENTITY_FILE_STRATEGY = new AllowableValue("identity-file", "Identity File",
            "Use an identity file to log in to SSH.  The file should be accessible by your NiFi installation and have appropriate permissions, typically chmod 600.");

    static final PropertyDescriptor SSH_HOST = new PropertyDescriptor.Builder().name("ssh-host").displayName("SSH Host")
            .description(
                    "SSH Host for Runrep Utility: This refers to the SSH host where the Geneva runrep utility is located. In most configurations, this is the same server that hosts your Geneva AGA. You should specify this as a hostname or IP address.")
            .required(true).validator(StandardValidators.HOSTNAME_VALIDATOR).build();

    static final PropertyDescriptor SSH_PORT = new PropertyDescriptor.Builder().name("ssh-port").displayName("SSH Port")
            .description("The port on the server to connect to. The default is 22.").required(true).defaultValue("22")
            .validator(StandardValidators.PORT_VALIDATOR).build();

    static final PropertyDescriptor SSH_AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("ssh-authentication-strategy").displayName("SSH Authentication Strategy")
            .description("Specifies the method of authentication for the SSH connection.")
            .allowableValues(USERNAME_PASSWORD_STRATEGY, IDENTITY_FILE_STRATEGY)
            .defaultValue(USERNAME_PASSWORD_STRATEGY.getValue()).required(true).build();

    static final PropertyDescriptor SSH_USERNAME = new PropertyDescriptor.Builder().name("ssh-username")
            .displayName("SSH Username")
            .description(
                    "The username on the host to connect as.  Note that this is (usually) not the same as your runrep username.")
            .dependsOn(SSH_AUTHENTICATION_STRATEGY, USERNAME_PASSWORD_STRATEGY).required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    static final PropertyDescriptor SSH_PASSWORD = new PropertyDescriptor.Builder().name("ssh-password")
            .displayName("SSH Password")
            .description(
                    "The password to connect to the host.  Note that this is (usually) not the same as your runrep password.")
            .dependsOn(SSH_AUTHENTICATION_STRATEGY, USERNAME_PASSWORD_STRATEGY).required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).sensitive(true).build();

    static final PropertyDescriptor IDENTITY_FILE = new PropertyDescriptor.Builder().name("identity-file")
            .displayName("Identity File")
            .description(
                    "The path to the SSH identity file.  Must be accessible by NiFi and have appropriate permissions.")
            .dependsOn(SSH_AUTHENTICATION_STRATEGY, IDENTITY_FILE_STRATEGY).required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    static final PropertyDescriptor RUNREP_USERNAME = new PropertyDescriptor.Builder().name("runrep-username")
            .displayName("Runrep Username").description("The username used to authenticate with runrep.").required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

    static final PropertyDescriptor RUNREP_PASSWORD = new PropertyDescriptor.Builder().name("runrep-password")
            .displayName("Runrep Password").description("The password used to authenticate with runrep.").required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

    static final PropertyDescriptor GENEVA_AGA = new PropertyDescriptor.Builder().name("geneva-aga")
            .displayName("Geneva AGA").description("Specifies the Geneva AGA (the port number) you want to target.").required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Any Geneva query that executed without errors will be routed to success").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Any Geneva query that executed without errors will be routed to failure").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(MY_PROPERTY);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        // TODO implement
    }
}
