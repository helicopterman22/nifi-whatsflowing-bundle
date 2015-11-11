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
package org.apache.nifi.processors.whatsflowing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({ "attributes", "logging", "whats", "flowing" })
@CapabilityDescription("This processor uses a regex to define which FlowFile attributes are logged as key/value pairs to a user defined log file.")
public class WhatsFlowing extends AbstractProcessor {

	public static final PropertyDescriptor LOG_FILE_PATH = new PropertyDescriptor.Builder()
			.name("Log File Directory")
			.description(
					"The directory of the log file. You may use expression language such as /aa/bb/${path}")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true).build();
			
	public static final PropertyDescriptor ATTRIBUTES_TO_LOG = new PropertyDescriptor.Builder()
			.name("Attributes to Log")
			.required(false)
			.description(
					"A regex defining a list of Attributes to Log.")
			.addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
			.defaultValue("file.*").build();


	private Set<Relationship> relationships;
	private List<PropertyDescriptor> supportedDescriptors;

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("All FlowFiles are routed to this relationship")
			.build();

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> procRels = new HashSet<>();
		procRels.add(REL_SUCCESS);
		relationships = Collections.unmodifiableSet(procRels);

		// descriptors
		final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
		supDescriptors.add(LOG_FILE_PATH);
		supDescriptors.add(ATTRIBUTES_TO_LOG);
		supportedDescriptors = Collections.unmodifiableList(supDescriptors);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return supportedDescriptors;
	}

	protected String processFlowFile(final Path rootDirPath,
			final ProcessorLog logger, final FlowFile flowFile, final ProcessSession session,
			final ProcessContext context) {
		

		final Set<String> attributeKeys = getAttributesToLog(flowFile
				.getAttributes().keySet(), context);
		final ProcessorLog LOG = getLogger();
				
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");    
		Date date = new Date(); 
		
		final String strDate = dateFormat.format(date);		
		final String logFileName = "WhatsFlowing_" + strDate + ".log";

		// Pretty print metadata
		final StringBuilder message = new StringBuilder();
		
		for (final String key : attributeKeys) 
		{			
			message.append(String.format("Key=%1$s Value=%2$s; ", key, flowFile.getAttribute(key)));
		}

		final String outputMessage = message.toString().trim();
		
		try {
			
			if (!Files.exists(rootDirPath)) {
				Files.createDirectories(rootDirPath);				
			} 

			File file = new File(rootDirPath + "/" + logFileName);
			 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(outputMessage);
			bw.newLine();
			bw.flush();
			bw.close();
			
			
		} catch (Exception e) {
			LOG.warn("Caught Exception " + e);
		}

		return outputMessage;

	}

	private Set<String> getAttributesToLog(final Set<String> flowFileAttrKeys,
			final ProcessContext context) {
		final Set<String> result = new TreeSet<>();
        final Pattern attributesPattern = Pattern.compile(context.getProperty(ATTRIBUTES_TO_LOG).getValue());
		Iterator <String> iterator = flowFileAttrKeys.iterator();
		while(iterator.hasNext()){
			final String attributeName = iterator.next();
			if (attributesPattern == null || attributesPattern.matcher(attributeName).matches()){
				result.add(attributeName);
			}
		}

		return result;
	}

	@Override
	public void onTrigger(final ProcessContext context,
			final ProcessSession session) {

		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

	
		final Path rootDirPath = Paths.get(context.getProperty(LOG_FILE_PATH)
				.evaluateAttributeExpressions(flowFile).getValue());

	
		final ProcessorLog LOG = getLogger();
		
		processFlowFile(rootDirPath, LOG, flowFile, session, context);

		session.transfer(flowFile, REL_SUCCESS);
	}

}
