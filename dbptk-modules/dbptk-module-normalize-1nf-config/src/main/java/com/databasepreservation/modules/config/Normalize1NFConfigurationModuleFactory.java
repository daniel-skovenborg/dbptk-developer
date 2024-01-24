/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.config;

import static java.util.stream.Collectors.toMap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import com.databasepreservation.managers.ModuleConfigurationManager;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.utils.ModuleConfigurationUtils;

/**
 * Exposes an export module which extends the import-config module to generate a
 * configuration which uses custom views normalize the database to be 1NF as
 * needed for SIARD DK.
 *
 * @author Daniel Lundsgaard Skovenborg <daniel.lundsgaard.skovenborg@stil.dk>
 */
public class Normalize1NFConfigurationModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_FILE = "file";
  public static final String NO_SQL_QUOTES = "no-sql-quotes";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to the import configuration file").hasArgument(true).setOptionalArgument(false).required(true);
  private static final Parameter noSQLQuotes = new Parameter().shortName("nqc").longName(NO_SQL_QUOTES)
    .description(
      "Don't quote SQL identifiers in normalization view queries (use if applicable to get more readable queries)")
    .hasArgument(false).required(false).valueIfNotSet("false").valueIfSet("true");

  @Override
  public boolean producesImportModules() {
    return false;
  }

  @Override
  public boolean producesExportModules() {
    return true;
  }

  @Override
  public String getModuleName() {
    return "normalize-1nf-config";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    return Stream.of(file, noSQLQuotes).collect(toMap(Parameter::longName, Function.identity()));
  }

  @Override
  public Parameters getConnectionParameters() throws UnsupportedModuleException {
    throw ExceptionBuilder.UnsupportedModuleExceptionForImportModule();
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    throw ExceptionBuilder.UnsupportedModuleExceptionForImportModule();
  }

  @Override
  public Parameters getExportModuleParameters() {
    return new Parameters(List.of(file, noSQLQuotes), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException {
    throw ExceptionBuilder.UnsupportedModuleExceptionForImportModule();
  }

  @Override
  public DatabaseFilterModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter) {
    Path pFile = Paths.get(parameters.get(file));
    boolean pNoSQLQuotes = Boolean.parseBoolean(parameters.get(noSQLQuotes));

    reporter.exportModuleParameters(this.getModuleName(), PARAMETER_FILE,
      pFile.normalize().toAbsolutePath().toString());

    final ModuleConfiguration defaultModuleConfiguration = ModuleConfigurationUtils.getDefaultModuleConfiguration();
    defaultModuleConfiguration.setFetchRows(false);
    defaultModuleConfiguration
      .setIgnore(ModuleConfigurationUtils.createIgnoreListExcept(true, DatabaseTechnicalFeatures.VIEWS));

    ModuleConfigurationManager.getInstance().setup(defaultModuleConfiguration);

    reporter.exportModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());

    return new Normalize1NFConfiguration(pFile, pNoSQLQuotes);
  }
}
