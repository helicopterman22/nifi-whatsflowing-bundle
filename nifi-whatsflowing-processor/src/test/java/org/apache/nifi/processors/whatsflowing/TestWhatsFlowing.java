package org.apache.nifi.processors.whatsflowing;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.whatsflowing.WhatsFlowing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

public class TestWhatsFlowing {

	@Test
	public void testValidators(){
		final TestRunner runner = TestRunners.newTestRunner(new WhatsFlowing());
		Collection <ValidationResult> results = new HashSet<>();
		ProcessContext pc;
		
		runner.enqueue(new byte[0]);
		pc = runner.getProcessContext();
		
		if (pc instanceof MockProcessContext){
			results = ((MockProcessContext)pc).validate();
		}
		assertEquals(1, results.size());
		for (ValidationResult vr : results){
			assertTrue(vr.toString().contains("Log File Directory is required"));
		}
		
		runner.setProperty(WhatsFlowing.LOG_FILE_PATH, "src/test/resources");
		runner.enqueue(new byte[0]);
		pc = runner.getProcessContext();
		
		if (pc instanceof MockProcessContext){
			results = ((MockProcessContext)pc).validate();
		}
		assertEquals(0, results.size());
	}
	
	@Test
	public void testSingleNiFiAttribute() throws IOException {
		//Do some setup and delete the log file if it exists
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
		Date date = new Date();
		final String LOG_PATH = "src/test/resources";
		final String strDate = dateFormat.format(date);
		final String logFileName = "WhatsFlowing_" + strDate + ".log";
		File logFile = new File (LOG_PATH + "/" + logFileName);
		
		logFile.delete();
		
		final TestRunner runner = TestRunners.newTestRunner(new WhatsFlowing());
		runner.setProperty(WhatsFlowing.ATTRIBUTES_TO_LOG, "filename");
		runner.setProperty(WhatsFlowing.LOG_FILE_PATH, LOG_PATH);

		runner.enqueue(Paths.get("src/test/resources/hello.txt"));
		runner.run();

		runner.assertAllFlowFilesTransferred(WhatsFlowing.REL_SUCCESS, 1);
		final MockFlowFile out = runner.getFlowFilesForRelationship(
				WhatsFlowing.REL_SUCCESS).get(0);
		out.assertContentEquals("Hello, World!".getBytes("UTF-8"));

		final String fileName = out.getAttribute("filename");
		//System.out.println("Filename = " + fileName);
		
		//Read and test the Logfile
		BufferedReader br = new BufferedReader(new FileReader(logFile));
		String logEntry = br.readLine();
		assertEquals(logEntry, "Key=filename" + " Value=" + fileName + ";");
		br.close();

	}

	
	@Test
	public void testMultipleNiFiAttributesWithRegex() throws IOException {
		//Do some setup and delete the log file if it exists
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
		Date date = new Date();
		final String LOG_PATH = "src/test/resources";
		final String strDate = dateFormat.format(date);
		final String logFileName = "WhatsFlowing_" + strDate + ".log";
		File logFile = new File (LOG_PATH + "/" + logFileName);
		
		logFile.delete();
		
		final Map<String,String> attributes = new HashMap<String,String>();
		attributes.put("filesize", "34");
		
		final TestRunner runner = TestRunners.newTestRunner(new WhatsFlowing());
		runner.setProperty(WhatsFlowing.ATTRIBUTES_TO_LOG, "file.*");
		runner.setProperty(WhatsFlowing.LOG_FILE_PATH, LOG_PATH);

		runner.enqueue(Paths.get("src/test/resources/hello.txt"),attributes);
		runner.run();

		runner.assertAllFlowFilesTransferred(WhatsFlowing.REL_SUCCESS, 1);
		final MockFlowFile out = runner.getFlowFilesForRelationship(
				WhatsFlowing.REL_SUCCESS).get(0);
		out.assertContentEquals("Hello, World!".getBytes("UTF-8"));

		final String fileName = out.getAttribute("filename");
		final String fileSize = out.getAttribute("filesize");
		
		/*final Map<String,String> flowAttributes = out.getAttributes();
		for (Map.Entry<String,String> entry : flowAttributes.entrySet()) {
		    String key = entry.getKey();
		    String value = entry.getValue();
		    System.out.println("Key  = " + key + "  Value = " + value);
		} */
				
		//Read and test the Logfile
		BufferedReader br = new BufferedReader(new FileReader(logFile));
		String logEntry = br.readLine();
		//System.out.println("Log entry = " + logEntry);
		assertEquals(logEntry, "Key=filename" + " Value=" + fileName + "; " + "Key=filesize" + " Value=" + fileSize + ";");
		br.close();

	}
	
	@Test
	public void testManyNiFiAttributesWithRegex() throws IOException {
		//Do some setup and delete the log file if it exists
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
		Date date = new Date();
		final String LOG_PATH = "src/test/resources";
		final String strDate = dateFormat.format(date);
		final String logFileName = "WhatsFlowing_" + strDate + ".log";
		File logFile = new File (LOG_PATH + "/" + logFileName);
		
		logFile.delete();
		
		final Map<String,String> attributes = new HashMap<String,String>();
		attributes.put("filesize", "34");
		
		final TestRunner runner = TestRunners.newTestRunner(new WhatsFlowing());
		runner.setProperty(WhatsFlowing.ATTRIBUTES_TO_LOG, "file.*|path");
		runner.setProperty(WhatsFlowing.LOG_FILE_PATH, LOG_PATH);

		runner.enqueue(Paths.get("src/test/resources/hello.txt"),attributes);
		runner.run();

		runner.assertAllFlowFilesTransferred(WhatsFlowing.REL_SUCCESS, 1);
		final MockFlowFile out = runner.getFlowFilesForRelationship(
				WhatsFlowing.REL_SUCCESS).get(0);
		out.assertContentEquals("Hello, World!".getBytes("UTF-8"));

		final String fileName = out.getAttribute("filename");
		final String fileSize = out.getAttribute("filesize");
		final String path = out.getAttribute("path");
			
		//Read and test the Logfile
		BufferedReader br = new BufferedReader(new FileReader(logFile));
		String logEntry = br.readLine();
		//System.out.println("Log entry = " + logEntry);
		assertEquals(logEntry, "Key=filename" + " Value=" + fileName + "; " + "Key=filesize" + " Value=" + fileSize + "; " + "Key=path" + " Value=" + path + ";");
		br.close();

	}
	
	
	@Ignore
	public void testFilePickedUp() throws IOException {
		final File directory = new File("target/test/data/in");
		deleteDirectory(directory);
		assertTrue(
				"Unable to create test data directory "
						+ directory.getAbsolutePath(), directory.exists()
						|| directory.mkdirs());

		final File inFile = new File("src/test/resources/hello.txt");
		final Path inPath = inFile.toPath();
		final File destFile = new File(directory, inFile.getName());
		final Path targetPath = destFile.toPath();
		final Path absTargetPath = targetPath.toAbsolutePath();
		final String absTargetPathStr = absTargetPath.getParent() + "/";
		Files.copy(inPath, targetPath);

		final TestRunner runner = TestRunners.newTestRunner(new WhatsFlowing());
		runner.setProperty(WhatsFlowing.LOG_FILE_PATH,
				directory.getAbsolutePath());
		runner.run();

		runner.assertAllFlowFilesTransferred(WhatsFlowing.REL_SUCCESS, 1);
		final List<MockFlowFile> successFiles = runner
				.getFlowFilesForRelationship(WhatsFlowing.REL_SUCCESS);
		successFiles.get(0).assertContentEquals(
				"Hello, World!".getBytes("UTF-8"));

		final String path = successFiles.get(0).getAttribute("path");
		assertEquals("/", path);
		final String absolutePath = successFiles.get(0).getAttribute(
				CoreAttributes.ABSOLUTE_PATH.key());
		assertEquals(absTargetPathStr, absolutePath);
	}

	private void deleteDirectory(final File directory) throws IOException {
		if (directory.exists()) {
			for (final File file : directory.listFiles()) {
				if (file.isDirectory()) {
					deleteDirectory(file);
				}

				assertTrue("Could not delete " + file.getAbsolutePath(),
						file.delete());
			}
		}
	}

	@Ignore
	public void testTodaysFilesPickedUp() throws IOException {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd",
				Locale.US);
		final String dirStruc = sdf.format(new Date());

		final File directory = new File("target/test/data/in/" + dirStruc);
		deleteDirectory(directory);
		assertTrue(
				"Unable to create test data directory "
						+ directory.getAbsolutePath(), directory.exists()
						|| directory.mkdirs());

		final File inFile = new File("src/test/resources/hello.txt");
		final Path inPath = inFile.toPath();
		final File destFile = new File(directory, inFile.getName());
		final Path targetPath = destFile.toPath();
		Files.copy(inPath, targetPath);

		final TestRunner runner = TestRunners.newTestRunner(new WhatsFlowing());
		runner.setProperty(WhatsFlowing.LOG_FILE_PATH,
				"target/test/data/in/${now():format('yyyy/MM/dd')}");
		runner.run();

		runner.assertAllFlowFilesTransferred(WhatsFlowing.REL_SUCCESS, 1);
		final List<MockFlowFile> successFiles = runner
				.getFlowFilesForRelationship(WhatsFlowing.REL_SUCCESS);
		successFiles.get(0).assertContentEquals(
				"Hello, World!".getBytes("UTF-8"));
	}

	@Ignore
	public void testPath() throws IOException {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/",
				Locale.US);
		final String dirStruc = sdf.format(new Date());

		final File directory = new File("target/test/data/in/" + dirStruc);
		deleteDirectory(new File("target/test/data/in"));
		assertTrue(
				"Unable to create test data directory "
						+ directory.getAbsolutePath(), directory.exists()
						|| directory.mkdirs());

		final File inFile = new File("src/test/resources/hello.txt");
		final Path inPath = inFile.toPath();
		final File destFile = new File(directory, inFile.getName());
		final Path targetPath = destFile.toPath();
		final Path absTargetPath = targetPath.toAbsolutePath();
		final String absTargetPathStr = absTargetPath.getParent().toString()
				+ "/";
		Files.copy(inPath, targetPath);

		final TestRunner runner = TestRunners.newTestRunner(new WhatsFlowing());
		runner.setProperty(WhatsFlowing.LOG_FILE_PATH, "target/test/data/in");
		runner.run();

		runner.assertAllFlowFilesTransferred(WhatsFlowing.REL_SUCCESS, 1);
		final List<MockFlowFile> successFiles = runner
				.getFlowFilesForRelationship(WhatsFlowing.REL_SUCCESS);
		successFiles.get(0).assertContentEquals(
				"Hello, World!".getBytes("UTF-8"));

		final String path = successFiles.get(0).getAttribute("path");
		assertEquals(dirStruc, path.replace('\\', '/'));
		final String absolutePath = successFiles.get(0).getAttribute(
				CoreAttributes.ABSOLUTE_PATH.key());
		assertEquals(absTargetPathStr, absolutePath);
	}

	@Ignore
	public void testAttributes() throws IOException {
		final File directory = new File("target/test/data/in/");
		deleteDirectory(directory);
		assertTrue(
				"Unable to create test data directory "
						+ directory.getAbsolutePath(), directory.exists()
						|| directory.mkdirs());

		final File inFile = new File("src/test/resources/hello.txt");
		final Path inPath = inFile.toPath();
		final File destFile = new File(directory, inFile.getName());
		final Path targetPath = destFile.toPath();
		Files.copy(inPath, targetPath);

		boolean verifyLastModified = false;
		try {
			destFile.setLastModified(1000000000);
			verifyLastModified = true;
		} catch (Exception donothing) {
		}

		boolean verifyPermissions = false;
		try {
			Files.setPosixFilePermissions(targetPath,
					PosixFilePermissions.fromString("r--r-----"));
			verifyPermissions = true;
		} catch (Exception donothing) {
		}

		final TestRunner runner = TestRunners.newTestRunner(new WhatsFlowing());
		runner.setProperty(WhatsFlowing.LOG_FILE_PATH, "target/test/data/in");
		runner.run();

		runner.assertAllFlowFilesTransferred(WhatsFlowing.REL_SUCCESS, 1);
		final List<MockFlowFile> successFiles = runner
				.getFlowFilesForRelationship(WhatsFlowing.REL_SUCCESS);

		if (verifyLastModified) {
			// try {
			// final DateFormat formatter = new
			// SimpleDateFormat(WhatsFlowing.FILE_MODIFY_DATE_ATTR_FORMAT,
			// Locale.US);
			// final Date fileModifyTime =
			// formatter.parse(successFiles.get(0).getAttribute("file.lastModifiedTime"));
			// assertEquals(new Date(1000000000), fileModifyTime);
			// } catch (ParseException e) {
			// fail();
			// }
		}
		if (verifyPermissions) {
			successFiles.get(0).assertAttributeEquals("file.permissions",
					"r--r-----");
		}
	}
}