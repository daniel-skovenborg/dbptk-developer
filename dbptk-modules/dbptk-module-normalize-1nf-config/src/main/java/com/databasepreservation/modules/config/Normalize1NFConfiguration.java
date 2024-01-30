package com.databasepreservation.modules.config;

import static com.databasepreservation.modules.config.Normalize1NFConfiguration.NormalizedColumnType.ARRAY;
import static com.databasepreservation.modules.config.Normalize1NFConfiguration.NormalizedColumnType.JSON;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.managers.ModuleConfigurationManager;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.configuration.*;
import com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.utils.MapUtils;
import com.databasepreservation.utils.ModuleConfigurationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * @author Daniel Lundsgaard Skovenborg <daniel.lundsgaard.skovenborg@stil.dk>
 */
public class Normalize1NFConfiguration extends ImportConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(Normalize1NFConfiguration.class);

  private static final Pattern TABLE_NAME_REGEXP = Pattern
    .compile("\\s(?:from|join)\\s+(\"[^\"]+\"|[^\\s.]+)\\s*\\.\\s*(\"[^\"]+\"|[^\\s.]+)");

  private static final String DEFAULT_ARRAY_NAME_PATTERN = "${table}__${column}";
  private static final String DEFAULT_ARRAY_FOREIGN_KEY_COLUMN_PATTERN = "${table}_${column}";
  private static final String DEFAULT_ARRAY_DESCRIPTION_PATTERN = "Normalized array column ${table}.${column}";
  private static final String DEFAULT_ARRAY_INDEX_COLUMN_NAME_PATTERN = "array_index";
  private static final String DEFAULT_ARRAY_ITEM_COLUMN_NAME_PATTERN = "${column}_item";
  private static final String DEFAULT_ARRAY_TABLE_ALIAS = "a";

  private static final String DEFAULT_JSON_NAME_PATTERN = DEFAULT_ARRAY_NAME_PATTERN;
  private static final String DEFAULT_JSON_FOREIGN_KEY_COLUMN_PATTERN = DEFAULT_ARRAY_FOREIGN_KEY_COLUMN_PATTERN;
  private static final String DEFAULT_JSON_DESCRIPTION_PATTERN = "Normalized JSON column ${table}.${column}";

  private String arrayNamePattern = DEFAULT_ARRAY_NAME_PATTERN;
  private String arrayForeignKeyColumnPattern = DEFAULT_ARRAY_FOREIGN_KEY_COLUMN_PATTERN;
  private String arrayDescriptionPattern = DEFAULT_ARRAY_DESCRIPTION_PATTERN;
  private String arrayIndexColumnNamePattern = DEFAULT_ARRAY_INDEX_COLUMN_NAME_PATTERN;
  private String arrayItemColumnNamePattern = DEFAULT_ARRAY_ITEM_COLUMN_NAME_PATTERN;
  private String arrayTableAlias = DEFAULT_ARRAY_TABLE_ALIAS;

  private String jsonNamePattern = DEFAULT_JSON_NAME_PATTERN;
  private String jsonForeignKeyColumnPattern = DEFAULT_JSON_FOREIGN_KEY_COLUMN_PATTERN;
  private String jsonDescriptionPattern = DEFAULT_JSON_DESCRIPTION_PATTERN;

  private final ModuleConfiguration mergeConfiguration;
  private final boolean noSQLQuotes;

  public Normalize1NFConfiguration(Path outputFile, Path mergeFile, boolean noSQLQuotes) throws ModuleException {
    super(outputFile);
    this.noSQLQuotes = noSQLQuotes;

    if (mergeFile == null) {
      // Create empty configuration so that we don't have to check for null everywhere.
      this.mergeConfiguration = new ModuleConfiguration();
    } else {
      try {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        this.mergeConfiguration = mapper.readValue(mergeFile.toFile(), ModuleConfiguration.class);
      } catch (IOException e) {
        throw new ModuleException()
          .withMessage("Could not read the merge configuration from file " + mergeFile.normalize().toAbsolutePath())
          .withCause(e);
      }
    }
  }

  @Override
  public void initDatabase() {
    super.initDatabase();

    ModuleConfiguration dbConfiguration = ModuleConfigurationManager.getInstance().getModuleConfiguration();
    Map<DatabaseTechnicalFeatures, Boolean> ignore = dbConfiguration.getIgnore();
    ignore.put(DatabaseTechnicalFeatures.PRIMARY_KEYS, false);
    dbConfiguration.setIgnore(ignore);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    super.handleDataOpenTable(tableId);

    String schemaName = currentSchema.getName();

    for (ColumnStructure column : currentTable.getColumns()) {
      boolean isArray = column.getType().getSql99TypeName().endsWith("ARRAY");
      boolean isJson = column.getType().getOriginalTypeName().equalsIgnoreCase("json")
        || column.getType().getOriginalTypeName().equalsIgnoreCase("jsonb");

      if (!isArray && !isJson)
        continue;

      NormalizedColumnType ncType = isArray ? ARRAY : JSON;
      String tableName = currentTable.getName();
      String columnName = column.getName();
      PrimaryKey primaryKey = currentTable.getPrimaryKey();

      if (primaryKey == null) {
        LOGGER.warn("Table {}.{} has no primary key. Cannot create normalization of {} column {}", ncType, schemaName,
          tableName, columnName);
        continue;
      }
      LOGGER.info("Creating normalization of {} column {}.{}.{}", ncType, schemaName, tableName, columnName);

      String viewName = formatTblCol(ncType == ARRAY ? arrayNamePattern : jsonNamePattern, tableName, columnName);
      String description = formatTblCol(ncType == ARRAY ? arrayDescriptionPattern : jsonDescriptionPattern, tableName,
        columnName);
      String query = ncType == ARRAY ? getArrayNormalizationSQL(schemaName, tableName, columnName, primaryKey)
        : getJsonNormalizationSQL(schemaName, tableName, primaryKey);
      PrimaryKeyConfiguration primaryKeyConfiguration = getPrimaryKeyConfiguration(ncType, primaryKey, tableName,
        columnName);
      ForeignKeyConfiguration foreignKeyConfiguration = getForeignKeyConfiguration(ncType, primaryKey, tableName);

      ModuleConfigurationUtils.addCustomViewConfiguration(moduleConfiguration, schemaName, viewName, true, description,
        query, primaryKeyConfiguration, Collections.singletonList(foreignKeyConfiguration));

      mergeViewConfiguration(schemaName, viewName);

      // Remove normalized column from table configuration.
      // Assume no copy.
      TableConfiguration tableConfiguration = moduleConfiguration.getTableConfiguration(schemaName, tableName);
      List<ColumnConfiguration> newColumns = tableConfiguration.getColumns().stream()
        .filter(c -> !c.getName().equals(columnName)).collect(Collectors.toList());
      tableConfiguration.setColumns(newColumns);
    }

    // TODO: add support for using merge configuration to add foreign keys to table,
    // e.g., foreign key from an enum column to a code table constructed with a
    // custom view in the merge configuration.
  }

  @Override
  public void finishDatabase() throws ModuleException {
    // Add all custom views from the merge configuration if not present.
    moduleConfiguration.getSchemaConfigurations().forEach((schemaName, schemaConfiguration) -> {
      List<CustomViewConfiguration> customViewConfigurations = schemaConfiguration.getCustomViewConfigurations();
      SchemaConfiguration schemaToMerge = mergeConfiguration.getSchemaConfigurations().get(schemaName);
      if (schemaToMerge == null)
        return;

      for (CustomViewConfiguration custom : schemaToMerge.getCustomViewConfigurations()) {
        if (moduleConfiguration.getCustomViewConfiguration(schemaName, custom.getName()) == null
          && validateCustomView(custom)) {
          LOGGER.info("Adding custom view {}.{} from merge configuration file", schemaName, custom.getName());
          customViewConfigurations.add(custom);
        }
      }
      customViewConfigurations.sort(Comparator.comparing(CustomViewConfiguration::getName));
    });

    super.finishDatabase();
  }

  private boolean validateCustomView(CustomViewConfiguration custom) {
    // Validate that the referenced tables exist in the schema. This is done to
    // avoid merging remnants of older versions of the schema.
    if (custom.getQuery() == null) {
      LOGGER.warn("Missing query in custom view {}", custom.getName());
      return false;
    }

    boolean valid = true;
    Matcher m = TABLE_NAME_REGEXP.matcher(custom.getQuery());
    while (m.find()) {
      String schemaName = m.group(1);
      String tableName = m.group(2);
      if (moduleConfiguration.getTableConfiguration(schemaName, tableName) == null) {
        LOGGER.warn("Custom view {} references non-existing table {}.{}", custom.getName(), schemaName, tableName);
        valid = false;
      }
    }

    return valid;
  }

  private void mergeViewConfiguration(String schemaName, String viewName) {
    CustomViewConfiguration merge = mergeConfiguration.getCustomViewConfiguration(schemaName, viewName);
    if (merge == null)
      return;

    LOGGER.info("Merging configuration of custom view {}.{}", schemaName, viewName);

    // Assume no copy on getters.
    // Allow setting description and columns, overriding query and primary key, and
    // adding foreign keys.
    CustomViewConfiguration view = moduleConfiguration.getCustomViewConfiguration(schemaName, viewName);
    if (merge.getDescription() != null) {
      view.setDescription(merge.getDescription());
    }
    if (merge.getQuery() != null) {
      view.setQuery(merge.getQuery());
    }
    if (merge.getColumns() != null) {
      view.setColumns(merge.getColumns());
    }
    if (merge.getPrimaryKey() != null) {
      view.setPrimaryKey(merge.getPrimaryKey());
    }
    if (merge.getForeignKeys() != null) {
      view.getForeignKeys().addAll(merge.getForeignKeys());
    }
  }

  private static String formatTblCol(String pattern, String tableName, String columnName) {
    return StringSubstitutor.replace(pattern, MapUtils.buildMapFromObjects("table", tableName, "column", columnName));
  }

  private String quoteSQL(String sqlName) {
    return noSQLQuotes ? sqlName : '"' + sqlName + '"';
  }

  private String getArrayNormalizationSQL(String schemaName, String tableName, String columnName,
    PrimaryKey primaryKey) {

    // Resulting SQL is only tested in PostgreSQL, but "UNNEST ... WITH ORDINALITY" should be standard SQL.
    String indexColumnName = formatTblCol(arrayIndexColumnNamePattern, tableName, columnName);
    String itemColumnName = formatTblCol(arrayItemColumnNamePattern, tableName, columnName);
    String qColumnName = quoteSQL(columnName);
    String qIndexColumnName = quoteSQL(indexColumnName);
    String qItemColumnName = quoteSQL(itemColumnName);

    StringBuilder sb = getNormalizationSQLStringBuilder(ARRAY, tableName, primaryKey).append(", ")
      .append(qIndexColumnName).append(", ").append(qItemColumnName).append(" ");
    addNormalizationSQLFrom(sb, schemaName, tableName);
    sb.append(" cross join unnest(").append(qColumnName).append(") with ordinality as ");
    sb.append(arrayTableAlias).append("(").append(qItemColumnName).append(", ").append(qIndexColumnName).append(")");

    return sb.toString();
  }

  private String getJsonNormalizationSQL(String schemaName, String tableName, PrimaryKey primaryKey) {
    // Get a template only; do not attempt to calculate columns which would require processing all rows in the table.
    StringBuilder sb = getNormalizationSQLStringBuilder(JSON, tableName, primaryKey).append(" ");
    addNormalizationSQLFrom(sb, schemaName, tableName);

    return sb.toString();
  }

  private StringBuilder getNormalizationSQLStringBuilder(NormalizedColumnType ncType, String tableName,
    PrimaryKey primaryKey) {

    StringBuilder sb = new StringBuilder("select");
    boolean first = true;

    for (String pkColumnName : primaryKey.getColumnNames()) {
      sb.append(first ? " " : ", ").append(pkColumnName).append(" as ")
        .append(quoteSQL(formatTblCol(ncType == ARRAY ? arrayForeignKeyColumnPattern : jsonForeignKeyColumnPattern,
          tableName, pkColumnName)));
      first = false;
    }

    return sb;
  }

  private StringBuilder addNormalizationSQLFrom(StringBuilder sb, String schemaName, String tableName) {
    String qSchemaName = quoteSQL(schemaName);
    String qTableName = quoteSQL(tableName);

    sb.append("from ").append(qSchemaName).append(".").append(qTableName);

    return sb;
  }

  private PrimaryKeyConfiguration getPrimaryKeyConfiguration(NormalizedColumnType ncType, PrimaryKey primaryKey,
    String tableName,
    String columnName) {
    PrimaryKeyConfiguration primaryKeyConfiguration = new PrimaryKeyConfiguration();
    List<String> columnNames = new ArrayList<>(2);

    for (String pkColumnName : primaryKey.getColumnNames()) {
      columnNames.add(formatTblCol(ncType == ARRAY ? arrayForeignKeyColumnPattern : jsonForeignKeyColumnPattern,
        tableName, pkColumnName));
    }

    if (ncType == ARRAY)
      columnNames.add(formatTblCol(arrayIndexColumnNamePattern, tableName, columnName));

    primaryKeyConfiguration.setColumnNames(columnNames);

    return primaryKeyConfiguration;
  }

  private ForeignKeyConfiguration getForeignKeyConfiguration(NormalizedColumnType ncType, PrimaryKey primaryKey,
    String tableName) {
    ForeignKeyConfiguration foreignKeyConfiguration = new ForeignKeyConfiguration();

    foreignKeyConfiguration.setReferencedTable(tableName);

    List<ReferenceConfiguration> refererences = new ArrayList<>(2);

    for (String pkColumnName : primaryKey.getColumnNames()) {
      ReferenceConfiguration ref = new ReferenceConfiguration();
      ref.setColumn(formatTblCol(ncType == ARRAY ? arrayForeignKeyColumnPattern : jsonForeignKeyColumnPattern,
        tableName, pkColumnName));
      ref.setReferenced(pkColumnName);
      refererences.add(ref);
    }
    foreignKeyConfiguration.setReferences(refererences);

    return foreignKeyConfiguration;
  }

  enum NormalizedColumnType {
    ARRAY, JSON
  }
}
