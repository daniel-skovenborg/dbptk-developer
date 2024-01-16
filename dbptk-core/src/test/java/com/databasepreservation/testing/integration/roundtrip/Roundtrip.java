/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.integration.roundtrip;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Main;
import com.databasepreservation.testing.integration.roundtrip.differences.DumpDiffExpectations;
import com.databasepreservation.utils.FileUtils;

/**
 * Core of siard roundtrip testing
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Roundtrip {
  public static final String TMP_FILE_SIARD_VAR = "%TMP_FILE_SIARD%";

  private static final Logger LOGGER = LoggerFactory.getLogger(Roundtrip.class);
  // constants
  private final String db_source = "dpttest";
  private final String db_target = "dpttest_siard";
  private final String db_tmp_username = "dpttest";
  private final String db_tmp_password = RandomStringUtils.randomAlphabetic(10);
  // set by constructor
  private String setup_command;
  private String teardown_command;
  private String populate_command;
  private String dump_source_command;
  private String dump_target_command;
  private String[] forward_conversion_arguments;
  private String[] backward_conversion_arguments;
  private DumpDiffExpectations dumpDiffExpectations;

  // used in populate step and when dumping source database
  private HashMap<String, String> environment_variables_source;
  // used when dumping target database
  private HashMap<String, String> environment_variables_target;

  // set internally at runtime
  private Path tmpFileSIARD;
  private Path tmpFolderSIARD;
  private File processSTDERR;
  private File processSTDOUT;

  public Roundtrip(String setup_command, String teardown_command, String populate_command, String dump_source_command,
    String dump_target_command, String[] forward_conversion_arguments, String[] backward_conversion_arguments,
    DumpDiffExpectations dumpDiffExpectations, HashMap<String, String> environment_variables_source,
    HashMap<String, String> environment_variables_target) throws IOException {
    this.setup_command = setup_command;
    this.populate_command = populate_command;
    this.teardown_command = teardown_command;
    this.dump_source_command = dump_source_command;
    this.dump_target_command = dump_target_command;
    this.forward_conversion_arguments = forward_conversion_arguments;
    this.backward_conversion_arguments = backward_conversion_arguments;
    this.dumpDiffExpectations = dumpDiffExpectations;
    this.environment_variables_source = environment_variables_source != null ? environment_variables_source
      : new HashMap<String, String>();
    this.environment_variables_target = environment_variables_target != null ? environment_variables_target
      : new HashMap<String, String>();

    processSTDERR = File.createTempFile("processSTDERR_", ".tmp");
    processSTDOUT = File.createTempFile("processSTDOUT_", ".tmp");
    processSTDERR.deleteOnExit();
    processSTDOUT.deleteOnExit();
  }

  /**
   * Sets up and tears down the roundtrip test environment. Asserting that
   * everything works.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public void checkConnection() throws IOException, InterruptedException {
    assert setup() == 0 : "Roundtrip setup exit status was not 0 (setup failed)";
    assert teardown() == 0 : "Roundtrip teardown exit status was not 0 (teardown failed)";
  }

  public boolean testTypeAndValue(String template, String... args) throws IOException, InterruptedException {
    // File populate_file = File.createTempFile("roundtrip_populate", ".sql");

    Path populate_file = Files.createTempFile("roundtrip_populate", ".sql");

    BufferedWriter bw = Files.newBufferedWriter(populate_file, StandardCharsets.UTF_8);

    bw.append(String.format(template, (Object[]) args));
    bw.newLine();
    bw.close();

    setup();
    boolean result = roundtrip(populate_file);
    teardown();
    Files.deleteIfExists(populate_file);
    return result;
  }

  public boolean testFile(Path populate_file) throws IOException, InterruptedException {
    assert setup() == 0 : "Roundtrip setup exit status was not 0";
    boolean result = roundtrip(populate_file);
    assert teardown() == 0 : "Roundtrip teardown exit status was not 0";
    return result;
  }

  /**
   * Runs a roundtrip test
   *
   * @param populate_file
   *          File with queries to populate the database
   * @return A diff string if the dumps differ, null otherwise
   * @throws IOException
   * @throws InterruptedException
   */
  private boolean roundtrip(Path populate_file) throws IOException, InterruptedException {
    boolean returnValue = false;

    ProcessBuilder sql = bashCommandProcessBuilder(populate_command);
    sql.redirectOutput(processSTDOUT);
    sql.redirectError(processSTDERR);
    sql.redirectInput(populate_file.toFile());
    for (Entry<String, String> entry : environment_variables_source.entrySet()) {
      sql.environment().put(entry.getKey(), entry.getValue());
    }
    Process p = sql.start();
    waitAndPrintTmpFileOnError(p, 600, processSTDERR, processSTDOUT);

    // We won't continue, if the process setting up the particular round trip
    // database didn't succeed.
    if (p.exitValue() != 0) {
      return false;
    }

    Path dumpsDir = Files.createTempDirectory("dpttest_dumps");

    Path dump_source = dumpsDir.resolve("source.sql");
    Path dump_target = dumpsDir.resolve("target.sql");

    LOGGER.trace("SQL src dump: " + dump_source.toString());
    LOGGER.trace("SQL tgt dump: " + dump_target.toString());

    ProcessBuilder dump = bashCommandProcessBuilder(dump_source_command);
    dump.redirectOutput(dump_source.toFile());
    dump.redirectError(processSTDERR);
    for (Entry<String, String> entry : environment_variables_source.entrySet()) {
      dump.environment().put(entry.getKey(), entry.getValue());
    }
    p = dump.start();
    waitAndPrintTmpFileOnError(p, 600, processSTDERR);

    // convert from the database to siard
    if (Main.internalMainUsedOnlyByTestClasses(reviewArguments(forward_conversion_arguments)) == 0) {
      // and if that succeeded, convert back to the database
      if (Main.internalMainUsedOnlyByTestClasses(reviewArguments(backward_conversion_arguments)) == 0) {
        // both conversions succeeded. going to compare the database dumps
        dump = bashCommandProcessBuilder(dump_target_command);
        dump.redirectOutput(dump_target.toFile());
        dump.redirectError(processSTDERR);
        for (Entry<String, String> entry : environment_variables_target.entrySet()) {
          dump.environment().put(entry.getKey(), entry.getValue());
        }
        p = dump.start();
        waitAndPrintTmpFileOnError(p, 600, processSTDERR);

        // this asserts that both dumps represent the same information
        try {
          dumpDiffExpectations.dumpsRepresentTheSameInformation(dump_source, dump_target);
        } catch (AssertionError e) {
          Files.deleteIfExists(dump_source);
          Files.deleteIfExists(dump_target);
          FileUtils.deleteDirectoryRecursive(dumpsDir);
          throw e;
        }

        returnValue = true;
      }
    }
    Files.deleteIfExists(dump_source);
    Files.deleteIfExists(dump_target);
    FileUtils.deleteDirectoryRecursive(dumpsDir);

    return returnValue;
  }

  private int setup() throws IOException, InterruptedException {
    // clean up before setting up
    ProcessBuilder teardown = bashCommandProcessBuilder(teardown_command);
    teardown.redirectOutput(processSTDOUT);
    teardown.redirectError(processSTDERR);
    Process p = teardown.start();
    waitAndPrintTmpFileOnError(p, 30, processSTDERR, processSTDOUT);

    // create a temporary folder with a siard file inside
    tmpFolderSIARD = Files.createTempDirectory("dpttest_siard");
    tmpFileSIARD = tmpFolderSIARD.resolve("dbptk.siard");
    LOGGER.trace("SIARD file: " + tmpFileSIARD.toString());

    // create user, database and give permissions to the user
    ProcessBuilder setup = bashCommandProcessBuilder(setup_command);
    setup.redirectOutput(processSTDOUT);
    setup.redirectError(processSTDERR);
    p = setup.start();

    waitAndPrintTmpFileOnError(p, 30, processSTDERR, processSTDOUT);
    return p.waitFor();
  }

  private int teardown() throws IOException, InterruptedException {
    FileUtils.deleteDirectoryRecursive(tmpFolderSIARD);
    Files.deleteIfExists(tmpFileSIARD);

    // clean up script
    ProcessBuilder teardown = bashCommandProcessBuilder(teardown_command);
    teardown.redirectOutput(processSTDOUT);
    teardown.redirectError(processSTDERR);

    Process p = teardown.start();
    waitAndPrintTmpFileOnError(p, 30, processSTDERR, processSTDOUT);
    return p.waitFor();
  }

  private String[] reviewArguments(String[] args) {
    String[] copy = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(TMP_FILE_SIARD_VAR)) {
        copy[i] = tmpFileSIARD.toString();
      } else {
        copy[i] = args[i];
      }
    }
    return copy;
  }

  private ProcessBuilder bashCommandProcessBuilder(String command) {
    LOGGER.debug("Command: {}", command);
    return new ProcessBuilder("bash", "-c", command);
  }

  private void waitAndPrintTmpFileOnError(Process p, long timeoutSeconds, File... files_to_print)
    throws IOException, InterruptedException {

    boolean completed = p.waitFor(timeoutSeconds, TimeUnit.SECONDS);
    if (!completed || p.exitValue() != 0) {
      if (!completed) {
        LOGGER.error("process timed out after {} seconds", timeoutSeconds);
        p.destroyForcibly().waitFor(30, TimeUnit.SECONDS);
      } else {
        LOGGER.error("non-zero exit code {}", p.exitValue());
      }

      for (File file_to_print : files_to_print) {
        printTmpFile(file_to_print);
      }
    }
  }

  private void printTmpFile(File file_to_print) throws IOException {
    LOGGER.error("printing process output from " + file_to_print.getName());

    if (file_to_print.length() > 0L) {
      FileReader fr;
      try {
        fr = new FileReader(file_to_print);
        try {
          BufferedReader br = new BufferedReader(fr);
          String line;
          while ((line = br.readLine()) != null) {
            System.out.println(line);
          }
          br.close();
        } catch (IOException e) {
          LOGGER.error("Could not read file", e);
        } finally {
          fr.close();
        }
      } catch (FileNotFoundException e) {
        LOGGER.error("File not found", e);
      }
    } else {
      LOGGER.warn("output file is empty.");
    }
  }
}
