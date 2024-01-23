package com.databasepreservation.modules.config;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import com.databasepreservation.utils.ModuleConfigurationUtils;

/**
 * @author Daniel Lundsgaard Skovenborg <daniel.lundsgaard.skovenborg@stil.dk>
 */
public class Normalize1NFConfiguration extends ImportConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(Normalize1NFConfiguration.class);
  private static final String DEFAULT_ARRAY_NAME_PATTERN = "${table}__${column}";
  private static final String DEFAULT_ARRAY_FOREIGN_KEY_COLUMN_PATTERN = "${table}_${column}";
  private static final String DEFAULT_ARRAY_DESCRIPTION_PATTERN = "Normalized array column ${table}.${column}";
  private static final String DEFAULT_ARRAY_INDEX_COLUMN_NAME_PATTERN = "array_index";
  private static final String DEFAULT_ARRAY_ITEM_COLUMN_NAME_PATTERN = "${column}_item";
  private static final String DEFAULT_ARRAY_TABLE_ALIAS = "a";

  private String arrayNamePattern = DEFAULT_ARRAY_NAME_PATTERN;
  private String arrayForeignKeyColumnPattern = DEFAULT_ARRAY_FOREIGN_KEY_COLUMN_PATTERN;
  private String arrayDescriptionPattern = DEFAULT_ARRAY_DESCRIPTION_PATTERN;
  private String arrayIndexColumnNamePattern = DEFAULT_ARRAY_INDEX_COLUMN_NAME_PATTERN;
  private String arrayItemColumnNamePattern = DEFAULT_ARRAY_ITEM_COLUMN_NAME_PATTERN;
  private String arrayTableAlias = DEFAULT_ARRAY_TABLE_ALIAS;

  public Normalize1NFConfiguration(Path outputFile) {
    super(outputFile);
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
      if (!column.getType().getSql99TypeName().endsWith("ARRAY"))
        continue;

      String tableName = currentTable.getName();
      String columnName = column.getName();
      PrimaryKey primaryKey = currentTable.getPrimaryKey();

      if (primaryKey == null) {
        LOGGER.warn("Table {}.{} has no primary key. Cannot create normalization of array column {}", schemaName,
          tableName, columnName);
        continue;
      }
      LOGGER.info("Creating normalization of array column {}.{}.{}", schemaName, tableName, columnName);

      String viewName = formatTblCol(arrayNamePattern, tableName, columnName);
      String description = formatTblCol(arrayDescriptionPattern, tableName, columnName);
      String query = getArrayNormalizationSQL(schemaName, tableName, columnName, primaryKey);
      PrimaryKeyConfiguration primaryKeyConfiguration = getPrimaryKeyConfiguration(primaryKey, tableName, columnName);
      ForeignKeyConfiguration foreignKeyConfiguration = getForeignKeyConfiguration(primaryKey, tableName);

      ModuleConfigurationUtils.addCustomViewConfiguration(moduleConfiguration, schemaName, viewName, true, description,
        query, primaryKeyConfiguration, Collections.singletonList(foreignKeyConfiguration));

      // Remove normalized column from table configuration.
      // Assume no copy.
      TableConfiguration tableConfiguration = moduleConfiguration.getTableConfiguration(schemaName, tableName);
      List<ColumnConfiguration> newColumns = tableConfiguration.getColumns().stream()
        .filter(c -> !c.getName().equals(columnName)).collect(Collectors.toList());
      tableConfiguration.setColumns(newColumns);
    }
  }

  private static String formatTblCol(String pattern, String tableName, String columnName) {
    return StringSubstitutor.replace(pattern, Map.of("table", tableName, "column", columnName));
  }

  private static String quoteSQL(String sqlName) {
    return '"' + sqlName + '"';
  }

  private String getArrayNormalizationSQL(String schemaName, String tableName, String columnName,
    PrimaryKey primaryKey) {

    // Resulting SQL is only tested in PostgreSQL, but "UNNEST ... WITH ORDINALITY" should be standard SQL.
    String indexColumnName = formatTblCol(arrayIndexColumnNamePattern, tableName, columnName);
    String itemColumnName = formatTblCol(arrayItemColumnNamePattern, tableName, columnName);
    String qSchemaName = quoteSQL(schemaName);
    String qTableName = quoteSQL(tableName);
    String qColumnName = quoteSQL(columnName);
    String qIndexColumnName = quoteSQL(indexColumnName);
    String qItemColumnName = quoteSQL(itemColumnName);

    StringBuilder sb = new StringBuilder("select ");
    for (String pkColumnName : primaryKey.getColumnNames()) {
      sb.append(pkColumnName).append(" as ")
        .append(quoteSQL(formatTblCol(arrayForeignKeyColumnPattern, tableName, pkColumnName))).append(", ");
    }

    sb.append(qIndexColumnName).append(", ").append(qItemColumnName);
    sb.append(" from ").append(qSchemaName).append(".").append(qTableName);
    sb.append(" cross join unnest(").append(qColumnName).append(") with ordinality as ");
    sb.append(arrayTableAlias).append("(").append(qItemColumnName).append(", ").append(qIndexColumnName).append(")");

    return sb.toString();
  }

  private PrimaryKeyConfiguration getPrimaryKeyConfiguration(PrimaryKey primaryKey, String tableName,
    String columnName) {
    PrimaryKeyConfiguration primaryKeyConfiguration = new PrimaryKeyConfiguration();
    List<String> columnNames = new ArrayList<>(2);

    for (String pkColumnName : primaryKey.getColumnNames()) {
      columnNames.add(formatTblCol(arrayForeignKeyColumnPattern, tableName, pkColumnName));
    }
    columnNames.add(formatTblCol(arrayIndexColumnNamePattern, tableName, columnName));
    primaryKeyConfiguration.setColumnNames(columnNames);

    return primaryKeyConfiguration;
  }

  private ForeignKeyConfiguration getForeignKeyConfiguration(PrimaryKey primaryKey, String tableName) {
    ForeignKeyConfiguration foreignKeyConfiguration = new ForeignKeyConfiguration();

    foreignKeyConfiguration.setReferencedTable(tableName);

    List<ReferenceConfiguration> refererences = new ArrayList<>(2);

    for (String pkColumnName : primaryKey.getColumnNames()) {
      ReferenceConfiguration ref = new ReferenceConfiguration();
      ref.setColumn(formatTblCol(arrayForeignKeyColumnPattern, tableName, pkColumnName));
      ref.setReferenced(pkColumnName);
      refererences.add(ref);
    }
    foreignKeyConfiguration.setReferences(refererences);

    return foreignKeyConfiguration;
  }
}
